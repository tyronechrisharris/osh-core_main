import re
import os

output = []
for root, _, files in os.walk("sensorhub-webui-core/src/main/java/org/sensorhub/ui"):
    for file in files:
        if file.endswith(".java"):
            filepath = os.path.join(root, file)
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    for i, line in enumerate(f):
                        # Regex to find hardcoded strings, trying to avoid common false positives
                        # Looks for strings starting with a capital letter, containing at least one space or multiple words
                        # and enclosed in double quotes.
                        matches = re.findall(r'"([A-Z][A-Za-z0-9]*(?:\s+[A-Za-z0-9]+)+[^"]*)"', line)
                        if matches:
                            for match in matches:
                                # Filter out very short strings or strings that are likely code/IDs
                                if len(match) > 4 and not re.match(r"^[A-Za-z0-9._-]+$", match):
                                    output.append(f"{filepath}:{i+1}:{match}")
            except Exception as e:
                output.append(f"Error reading {filepath}: {e}")

with open("/tmp/hardcoded_strings_python.txt", "w", encoding="utf-8") as outfile:
    for line in output:
        outfile.write(line + "\n")

print("Processing complete. Results in /tmp/hardcoded_strings_python.txt")
