package com.minymaton

import android.content.Context
import android.security.keystore.KeyGenParameterSpec
import android.security.keystore.KeyProperties
import android.util.Base64
import java.security.KeyStore
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec

/**
 * Bus tokens, sealed with an AES-256-GCM key that never leaves the Android
 * Keystore. Only the ciphertext and its IV reach app storage; the profile id
 * is the associated data, so a sealed token cannot be moved to another profile.
 */
class MinymatonKeyStore(context: Context) {
    private val preferences =
        context.getSharedPreferences("minymaton_bus_tokens", Context.MODE_PRIVATE)

    private companion object {
        const val PROVIDER = "AndroidKeyStore"
        const val ALIAS = "minymaton-bus-tokens"
        const val TRANSFORMATION = "AES/GCM/NoPadding"
        const val TAG_BITS = 128
    }

    private fun key(): SecretKey {
        val keyStore = KeyStore.getInstance(PROVIDER).apply { load(null) }
        (keyStore.getKey(ALIAS, null) as SecretKey?)?.let { return it }
        val generator = KeyGenerator.getInstance(KeyProperties.KEY_ALGORITHM_AES, PROVIDER)
        generator.init(
            KeyGenParameterSpec.Builder(
                ALIAS,
                KeyProperties.PURPOSE_ENCRYPT or KeyProperties.PURPOSE_DECRYPT,
            )
                .setBlockModes(KeyProperties.BLOCK_MODE_GCM)
                .setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE)
                .setKeySize(256)
                .build(),
        )
        return generator.generateKey()
    }

    fun save(profileId: String, token: String) {
        val cipher = Cipher.getInstance(TRANSFORMATION)
        cipher.init(Cipher.ENCRYPT_MODE, key())
        cipher.updateAAD(profileId.toByteArray(Charsets.UTF_8))
        val sealed = cipher.doFinal(token.toByteArray(Charsets.UTF_8))
        val encoded = Base64.encodeToString(cipher.iv, Base64.NO_WRAP) + ":" +
            Base64.encodeToString(sealed, Base64.NO_WRAP)
        if (!preferences.edit().putString(profileId, encoded).commit()) {
            throw java.security.GeneralSecurityException("cannot store the sealed token")
        }
    }

    fun load(profileId: String): String? {
        val encoded = preferences.getString(profileId, null) ?: return null
        val (iv, sealed) = encoded.split(":", limit = 2).map { Base64.decode(it, Base64.NO_WRAP) }
        val cipher = Cipher.getInstance(TRANSFORMATION)
        cipher.init(Cipher.DECRYPT_MODE, key(), GCMParameterSpec(TAG_BITS, iv))
        cipher.updateAAD(profileId.toByteArray(Charsets.UTF_8))
        return String(cipher.doFinal(sealed), Charsets.UTF_8)
    }

    fun delete(profileId: String) {
        preferences.edit().remove(profileId).commit()
    }
}
