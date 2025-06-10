OpenSensorHub
===========================================================

[![Build Status](https://github.com/opensensorhub/osh-core/actions/workflows/gradle_build.yml/badge.svg)](https://github.com/opensensorhub/osh-core/actions/workflows/gradle_build.yml)
[![GitHub Release](https://img.shields.io/github/release/opensensorhub/osh-core.svg)](https://github.com/opensensorhub/osh-core/releases/latest)
[![OpenSensorHub Discord](https://user-images.githubusercontent.com/7288322/34429117-c74dbd12-ecb8-11e7-896d-46369cd0de5b.png)](https://discord.gg/6k3QYRSh9F)

OpenSensorHub (OSH) software allows one to easily build interoperable and evolutive sensor networks with advanced processing capabilities and based on open standards for all data exchanges. These open-standards are mostly [OGC](http://www.opengeospatial.org) standards from the [Sensor Web Enablement](http://www.opengeospatial.org/projects/groups/sensorwebdwg) (SWE) initiative and are key to design sensor networks that can largely evolve with time (addition of new types of sensors, reconfigurations, etc.).

The framework allows one to connect any kind of sensor and actuator to a common bus via a simple yet generic driver API. Sensors can be connected through any available hardware interface such as [RS232/422](http://en.wikipedia.org/wiki/RS-232), [SPI](http://en.wikipedia.org/wiki/Serial_Peripheral_Interface_Bus), [I2C](http://en.wikipedia.org/wiki/I%C2%B2C), [USB](http://en.wikipedia.org/wiki/USB), [Ethernet](http://en.wikipedia.org/wiki/Ethernet), [Wifi](http://en.wikipedia.org/wiki/Wi-Fi), [Bluetooth](http://en.wikipedia.org/wiki/Bluetooth), [ZigBee](http://en.wikipedia.org/wiki/ZigBee), [HTTP](http://en.wikipedia.org/wiki/Hypertext_Transfer_Protocol), etc... Once drivers are available for a specific sensor, it is automatically connected to the bus, and it is then trivial to send commands and read data from it. An intuitive user interface allows the user to configure the network to suit its needs, and more advanced processing capabilities are available via a plugin system.

OSH embeds the full power of OGC web services ([Sensor Observation Service](http://www.opengeospatial.org/standards/sos) or SOS, [Sensor Planning Service](http://www.opengeospatial.org/standards/sps) or SPS) to communicate with all connected sensors in the network as well as to provide robust metadata (owner, location and orientation, calibration, etc.) about them. Through these standards, several SensorHub instances can also communicate with each other to form larger networks.

Low level functions of SensorHub (send commands and read data from sensor) are coded efficiently and can be used on embedded hardware running [Java SE®](http://www.oracle.com/technetwork/java/javase), [Java ME®](http://www.oracle.com/technetwork/java/embedded/javame) or [Android®](http://www.android.com) while more advanced data processing capabilities are fully multi-threaded and can thus benefit from a more powerful hardware platform (e.g. multi-processor servers or even clusters).


## License

OpenSensorHub is licensed under the [Mozilla Public License version 2.0](http://www.mozilla.org/MPL/2.0/).


## Using

Refer to the [Documentation Site](http://docs.opensensorhub.org/) for instructions on how to install and use OSH, as well as get the latest news.

You can also go to this [Demo Page](http://opensensorhub.github.io/osh-js/latest/showcase/) to see OSH in action with a few example sensor streams (Video, GPS, orientation, weather, etc.) visualized within simple javascript clients.

This other [Technical Page](http://sensiasoft.net:8181/demo.html) contains example SWE service calls for you to see the standard compliant XML and data that OSH generates.


## Building

OpenSensorHub can be built using Gradle, either from the command line or within Eclipse. Please see the [Developer's Guide](http://docs.opensensorhub.org/dev/dev-setup) for detailed instructions.


## Contributing

Refer to the [Developer's Guide](https://docs.opensensorhub.org/dev/dev-guide) for instructions on how to setup your development environment.

The Developer's guide includes information on how to build with Eclipse or Gradle.

You can also find useful information in the [Javadocs](http://docs.opensensorhub.org/dev/javadoc/) and Design Documentation on the [Wiki](../../wiki/Home). 

Several sensor driver examples are also available in the source code to help you get started.

Join our Discord to offer feedback!

## Internationalization (i18n)

This application supports multiple languages using Java's built-in `ResourceBundle` mechanism. The user interface text is dynamically updated based on the user's selected language preference.

### How it Works

- User interface strings are stored in properties files located in `sensorhub-webui-core/src/main/resources/org/sensorhub/ui/`.
- The default language file is `messages.properties` (English).
- Each supported language has its own properties file named `messages_xx.properties`, where `xx` is the two-letter ISO 639-1 language code (e.g., `messages_es.properties` for Spanish).
- When the application starts, it loads the appropriate resource bundle based on the user's saved language preference or the default system locale.
- UI components then fetch strings from this bundle using keys (e.g., `resourceBundle.getString("adminUI.title")`).
- A language selector is available in the application header, allowing users to switch languages dynamically. The selected preference is saved for future sessions.

### Adding a New Language

To add support for a new language, follow these steps:

1.  **Identify the ISO 639-1 Code:** Determine the two-letter code for the language you want to add (e.g., `pt` for Portuguese, `it` for Italian).
2.  **Create a New Properties File:**
    *   Navigate to the `sensorhub-webui-core/src/main/resources/org/sensorhub/ui/` directory.
    *   Make a copy of the `messages.properties` file.
    *   Rename the copied file to `messages_xx.properties`, replacing `xx` with the new language code (e.g., `messages_pt.properties`).
3.  **Translate the Strings:**
    *   Open the newly created `messages_xx.properties` file.
    *   Translate all the string values (the text after the `=` sign) into the new language. Keep the keys (the text before the `=` sign) exactly the same as in `messages.properties`.
    *   Example for `messages_pt.properties`:
        ```properties
        adminUI.title=OpenSensorHub [pt] # Replace with actual Portuguese translation
        adminUI.sensorsTab=Sensores [pt] # Replace with actual Portuguese translation
        # ... and so on for all other keys
        ```
4.  **Update Supported Locales (Code Change):**
    *   Open the `AdminUI.java` file located in `sensorhub-webui-core/src/main/java/org/sensorhub/ui/`.
    *   Locate the `buildHeader()` method.
    *   Add the new `Locale` and its display name to the `supportedLocales` map. For example, for Portuguese:
        ```java
        supportedLocales.put(new Locale("pt"), "Português");
        ```
    *   Locate the `isSupportedLocale(Locale locale)` method in `AdminUI.java`.
    *   Add the new language code to the checks. For example, for Portuguese:
        ```java
        return locale.getLanguage().equals(Locale.ENGLISH.getLanguage()) ||
               locale.getLanguage().equals("es") ||
               locale.getLanguage().equals(Locale.FRENCH.getLanguage()) ||
               locale.getLanguage().equals(Locale.GERMAN.getLanguage()) ||
               locale.getLanguage().equals("pt"); // Add new language here
        ```
    *   (Optional but recommended for robustness) If `AppSessionInitListener.java` is actively used and registered, update its `isSupported` method similarly and potentially the logic for adding the new locale to `event.getSession().setLocale(loadedLocale);` checks if you want fine-grained control over supported language tags there.
5.  **Test:** Rebuild and run the application. Select the new language from the language selector and verify that all UI text is correctly translated.

By following these steps, you can extend the application's internationalization capabilities.
