#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "Building CFNB for iOS/arm64..."

if ! command -v xcrun &> /dev/null; then
    echo "Xcode not found. Cannot build for iOS."
    exit 1
fi

SDK_PATH=$(xcrun --sdk iphoneos --show-sdk-path 2>/dev/null || echo "")
if [ -z "$SDK_PATH" ]; then
    echo "iOS SDK not found."
    exit 1
fi

echo "iOS SDK found at: $SDK_PATH"

cd cfnb-go

CGO_ENABLED=1 \
GOOS=darwin \
GOARCH=arm64 \
CC="$(xcrun -sdk iphoneos find clang)" \
CXX="$(xcrun -sdk iphoneos find clang++)" \
CFLAGS="-arch arm64 -isysroot $SDK_PATH -miphoneos-version-min=12.0" \
CXXFLAGS="-arch arm64 -isysroot $SDK_PATH -miphoneos-version-min=12.0" \
go build -o cfnb . 2>&1

echo "Build output:"
file cfnb

# Find the iOS SDK path
SDK_PATH=$(xcrun --sdk iphoneos --show-sdk-path 2>/dev/null || echo "")
if [ -z "$SDK_PATH" ]; then
    echo "iOS SDK not found. Make sure Xcode Command Line Tools are installed."
    exit 1
fi

echo "iOS SDK found at: $SDK_PATH"

# Cross-compile Go binary for iOS arm64
CGO_ENABLED=1 \
GOOS=darwin \
GOARCH=arm64 \
CC="$(xcrun -sdk iphoneos find clang)" \
CXX="$(xcrun -sdk iphoneos find clang++)" \
CFLAGS="-arch arm64 -isysroot $SDK_PATH -miphoneos-version-min=12.0" \
CXXFLAGS="-arch arm64 -isysroot $SDK_PATH -miphoneos-version-min=12.0" \
go build -o cfnb . 2>&1

if [ $? -eq 0 ]; then
    echo "Build successful!"
    file cfnb
else
    echo "Build failed."
    exit 1
fi
