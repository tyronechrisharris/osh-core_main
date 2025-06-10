#!/bin/bash
RESOURCE_DIR="sensorhub-webui-core/src/main/resources/org/sensorhub/ui"
DEFAULT_BUNDLE="${RESOURCE_DIR}/messages.properties"
LANG="ru"
TARGET_BUNDLE="${RESOURCE_DIR}/messages_${LANG}.properties"

# Check if default bundle exists
if [ ! -f "${DEFAULT_BUNDLE}" ]; then
    echo "Default resource bundle ${DEFAULT_BUNDLE} not found!"
    exit 1
fi

echo "Creating ${TARGET_BUNDLE}..."
# Copy keys and add placeholder Russian translations
awk -F'=' '{printf "%s=[%s] %s\n", $1, ENVIRON["LANG"], $2}' LANG="${LANG}" < "${DEFAULT_BUNDLE}" > "${TARGET_BUNDLE}"

if [ $? -eq 0 ]; then
    echo "Successfully created and populated ${TARGET_BUNDLE} with placeholder Russian translations."
else
    echo "Failed to create or populate ${TARGET_BUNDLE}."
    exit 1
fi

echo "Russian resource bundle creation complete."
