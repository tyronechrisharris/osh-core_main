package org.sensorhub.ui;

import com.vaadin.server.SessionInitEvent;
import com.vaadin.server.SessionInitListener;
import com.vaadin.server.VaadinSession;
import java.util.Locale;
import java.util.prefs.Preferences;

public class AppSessionInitListener implements SessionInitListener {

    private static final String LANGUAGE_PREF_KEY = "userLanguage";

    @Override
    public void sessionInit(SessionInitEvent event) {
        Preferences prefs = Preferences.userNodeForPackage(AdminUI.class);
        String savedLanguageTag = prefs.get(LANGUAGE_PREF_KEY, null);

        if (savedLanguageTag != null) {
            try {
                Locale loadedLocale = Locale.forLanguageTag(savedLanguageTag);
                // Ensure the loaded locale is one of the supported ones to prevent issues
                // This check can be made more robust by comparing against the actual list of supported locales
                if (loadedLocale.getLanguage().equals(Locale.ENGLISH.getLanguage()) ||
                    loadedLocale.getLanguage().equals("es") ||
                    loadedLocale.getLanguage().equals(Locale.FRENCH.getLanguage()) ||
                    loadedLocale.getLanguage().equals(Locale.GERMAN.getLanguage())) {
                    event.getSession().setLocale(loadedLocale);
                } else {
                    // Log that an unsupported locale was found in prefs, using default
                    System.err.println("Found unsupported language preference: " + savedLanguageTag + ". Using default.");
                     // Fallback to English if current default is not already set by Vaadin based on browser
                    if (event.getSession().getLocale() == null || !isSupported(event.getSession().getLocale())) {
                         event.getSession().setLocale(Locale.ENGLISH);
                    }
                }
            } catch (Exception e) {
                // Log error reading preference
                System.err.println("Error processing saved language preference: " + savedLanguageTag + ". Error: " + e.getMessage());
                if (event.getSession().getLocale() == null || !isSupported(event.getSession().getLocale())) {
                    event.getSession().setLocale(Locale.ENGLISH);
                }
            }
        } else {
            // If no preference, ensure a supported default (e.g., English if browser default isn't supported)
             if (event.getSession().getLocale() == null || !isSupported(event.getSession().getLocale())) {
                event.getSession().setLocale(Locale.ENGLISH);
            }
        }
    }

    private boolean isSupported(Locale locale) {
        if (locale == null) return false;
        return locale.getLanguage().equals(Locale.ENGLISH.getLanguage()) ||
               locale.getLanguage().equals("es") ||
               locale.getLanguage().equals(Locale.FRENCH.getLanguage()) ||
               locale.getLanguage().equals(Locale.GERMAN.getLanguage());
    }
}
