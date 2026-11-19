package com.minymaton

import android.util.Log
import com.facebook.react.bridge.Promise
import com.facebook.react.bridge.ReactApplicationContext
import com.facebook.react.bridge.ReactContextBaseJavaModule
import com.facebook.react.bridge.ReactMethod
import com.facebook.react.modules.core.DeviceEventManagerModule
import org.json.JSONObject
import java.io.File
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

/** Receives core events as JSON, on a core thread. */
interface MinymatonListener {
    fun onEvent(event: String)
}

/**
 * The bridge between the React UI and the Rust core (crates/minymaton-mobile-core,
 * src/android.rs). Commands are JSON documents answered with
 * `{ok, result}` or `{ok: false, error: {code, message}}`; they block, so they
 * run in order on one worker thread. Bus tokens never reach the core's
 * database: they live in [MinymatonKeyStore] and are handed to `activate`.
 */
class MinymatonModule(private val context: ReactApplicationContext) :
    ReactContextBaseJavaModule(context) {

    companion object {
        init {
            System.loadLibrary("minymaton_mobile_core")
        }

        const val EVENT = "MinymatonEvent"
        const val LOG_TAG = "MinymatonCore"

        @JvmStatic
        external fun nativeOpen(dataDir: String, listener: MinymatonListener): Long

        @JvmStatic
        external fun nativeRequest(handle: Long, command: String): String

        @JvmStatic
        external fun nativeClose(handle: Long)
    }

    private val worker: ExecutorService = Executors.newSingleThreadExecutor()
    private val keyStore = MinymatonKeyStore(context)
    private val listeners = AtomicInteger(0)
    private val handle: Long = nativeOpen(
        File(context.filesDir, "minymaton").absolutePath,
        object : MinymatonListener {
            override fun onEvent(event: String) = emit(event)
        },
    )

    override fun getName(): String = "MinymatonModule"

    private fun emit(event: String) {
        // The kind only, never message content: lets a host observe the core.
        Log.i(LOG_TAG, "event " + JSONObject(event).optString("event"))
        if (listeners.get() == 0 || !context.hasActiveReactInstance()) {
            return
        }
        context
            .getJSModule(DeviceEventManagerModule.RCTDeviceEventEmitter::class.java)
            .emit(EVENT, event)
    }

    @ReactMethod
    fun request(command: String, promise: Promise) {
        worker.execute {
            try {
                promise.resolve(nativeRequest(handle, command))
            } catch (error: IllegalStateException) {
                promise.reject("core_failed", error.message, error)
            }
        }
    }

    @ReactMethod
    fun saveToken(profileId: String, token: String, promise: Promise) {
        worker.execute {
            try {
                keyStore.save(profileId, token)
                promise.resolve(null)
            } catch (error: java.security.GeneralSecurityException) {
                promise.reject("keystore_failed", error.message, error)
            }
        }
    }

    @ReactMethod
    fun loadToken(profileId: String, promise: Promise) {
        worker.execute {
            try {
                promise.resolve(keyStore.load(profileId))
            } catch (error: java.security.GeneralSecurityException) {
                promise.reject("keystore_failed", error.message, error)
            }
        }
    }

    @ReactMethod
    fun deleteToken(profileId: String, promise: Promise) {
        worker.execute {
            keyStore.delete(profileId)
            promise.resolve(null)
        }
    }

    // NativeEventEmitter reports its subscriptions; core events are only
    // forwarded while JS is listening for them.
    @ReactMethod
    fun addListener(eventName: String) {
        if (eventName == EVENT) {
            listeners.incrementAndGet()
        }
    }

    @ReactMethod
    fun removeListeners(count: Double) {
        listeners.updateAndGet { current -> maxOf(0, current - count.toInt()) }
    }

    override fun invalidate() {
        // Close runs after every request already queued, then the worker stops.
        worker.execute { nativeClose(handle) }
        worker.shutdown()
        super.invalidate()
    }
}
