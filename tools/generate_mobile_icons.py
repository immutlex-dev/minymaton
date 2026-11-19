#!/usr/bin/env python3
"""Rasterize assets/logo.svg into the Android and iOS app icons.

Deterministic: renders the SVG once at 2048px wide with resvg, then
downscales with Pillow (Lanczos) onto a #f8fafc background. Android round
icons get an anti-aliased circular mask; iOS icons are opaque RGB.

Run with the repo venv:  .venv/bin/python tools/generate_mobile_icons.py
Node deps (one-time):    npm --prefix tools install
"""
import io
import json
import pathlib
import subprocess
import sys

from PIL import Image, ImageDraw

ROOT = pathlib.Path(__file__).resolve().parent.parent
SVG = ROOT / "assets" / "logo.svg"
BACKGROUND = (248, 250, 252)  # #f8fafc
MASTER_WIDTH = 2048

ANDROID_SIZES = {
    "mipmap-mdpi": 48,
    "mipmap-hdpi": 72,
    "mipmap-xhdpi": 96,
    "mipmap-xxhdpi": 144,
    "mipmap-xxxhdpi": 192,
}
IOS_ICONS = [
    ("Icon-20@2x.png", "iphone", "20x20", "2x", 40),
    ("Icon-20@3x.png", "iphone", "20x20", "3x", 60),
    ("Icon-29@2x.png", "iphone", "29x29", "2x", 58),
    ("Icon-29@3x.png", "iphone", "29x29", "3x", 87),
    ("Icon-40@2x.png", "iphone", "40x40", "2x", 80),
    ("Icon-40@3x.png", "iphone", "40x40", "3x", 120),
    ("Icon-60@2x.png", "iphone", "60x60", "2x", 120),
    ("Icon-60@3x.png", "iphone", "60x60", "3x", 180),
    ("Icon-1024.png", "ios-marketing", "1024x1024", "1x", 1024),
]


def render_master() -> Image.Image:
    node_modules = ROOT / "tools" / "node_modules"
    if not (node_modules / "@resvg" / "resvg-js").exists():
        subprocess.run(
            ["npm", "--prefix", str(ROOT / "tools"), "install", "--no-audit", "--no-fund"],
            check=True,
        )
    script = (
        "const {Resvg} = require(process.argv[1]);"
        "const fs = require('fs');"
        "const svg = fs.readFileSync(process.argv[2]);"
        "const resvg = new Resvg(svg, {fitTo: {mode: 'width', value: Number(process.argv[3])}});"
        "process.stdout.write(resvg.render().asPng());"
    )
    result = subprocess.run(
        ["node", "-e", script,
         str(node_modules / "@resvg" / "resvg-js"),
         str(SVG), str(MASTER_WIDTH)],
        check=True, capture_output=True,
    )
    return Image.open(io.BytesIO(result.stdout)).convert("RGBA")


def fit_on_canvas(master: Image.Image, size: int, width_fraction: float) -> Image.Image:
    canvas = Image.new("RGBA", (size, size), BACKGROUND + (255,))
    target_w = max(1, round(size * width_fraction))
    target_h = max(1, round(target_w * master.height / master.width))
    logo = master.resize((target_w, target_h), Image.LANCZOS)
    canvas.paste(logo, ((size - target_w) // 2, (size - target_h) // 2), logo)
    return canvas


def android_icons(master: Image.Image) -> None:
    res = ROOT / "mobile" / "android" / "app" / "src" / "main" / "res"
    for folder, size in ANDROID_SIZES.items():
        fit_on_canvas(master, size, 0.84).save(res / folder / "ic_launcher.png")
        round_canvas = Image.new("RGBA", (size, size), (0, 0, 0, 0))
        inner = fit_on_canvas(master, size, 0.68)
        mask = Image.new("L", (size * 4, size * 4), 0)
        ImageDraw.Draw(mask).ellipse([0, 0, size * 4, size * 4], fill=255)
        round_canvas.paste(inner, (0, 0), mask.resize((size, size), Image.LANCZOS))
        round_canvas.save(res / folder / "ic_launcher_round.png")


def ios_icons(master: Image.Image) -> None:
    appicon = ROOT / "mobile" / "ios" / "Minymaton" / "Images.xcassets" / "AppIcon.appiconset"
    images = []
    for filename, idiom, points, scale, pixels in IOS_ICONS:
        fit_on_canvas(master, pixels, 0.84).convert("RGB").save(appicon / filename)
        images.append({"filename": filename, "idiom": idiom, "scale": scale, "size": points})
    contents = {"images": images, "info": {"author": "xcode", "version": 1}}
    (appicon / "Contents.json").write_text(json.dumps(contents, indent=2) + "\n")


def main() -> int:
    master = render_master()
    android_icons(master)
    ios_icons(master)
    print(f"icons generated from {SVG.name} at {master.width}x{master.height}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
