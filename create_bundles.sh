#!/bin/bash
RESOURCE_DIR="sensorhub-webui-core/src/main/resources/org/sensorhub/ui"
DEFAULT_BUNDLE="${RESOURCE_DIR}/messages.properties"
LANGUAGES=("es" "fr" "de")

# Check if default bundle exists
if [ ! -f "${DEFAULT_BUNDLE}" ]; then
    echo "Default resource bundle ${DEFAULT_BUNDLE} not found!"
    exit 1
fi

for lang in "${LANGUAGES[@]}"; do
    TARGET_BUNDLE="${RESOURCE_DIR}/messages_${lang}.properties"
    echo "Creating ${TARGET_BUNDLE}..."
    # Copy keys and add placeholder translations
    awk -F'=' '{printf "%s=[%s] %s\n", $1, ENVIRON["lang"], $2}' lang="${lang}" < "${DEFAULT_BUNDLE}" > "${TARGET_BUNDLE}"
    if [ $? -eq 0 ]; then
        echo "Successfully created and populated ${TARGET_BUNDLE} with placeholder translations."
    else
        echo "Failed to create or populate ${TARGET_BUNDLE}."
        exit 1
    fi
done

echo "Language resource bundle creation complete."
