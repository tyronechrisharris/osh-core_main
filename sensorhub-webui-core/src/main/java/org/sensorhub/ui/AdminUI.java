/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.ui;

import java.text.MessageFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.prefs.Preferences;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Flow.Subscription;
import javax.servlet.ServletContext;
import org.sensorhub.api.ISensorHub;
import org.sensorhub.api.client.ClientConfig;
import org.sensorhub.api.comm.NetworkConfig;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.data.IDataProducerModule;
import org.sensorhub.api.database.DatabaseConfig;
import org.sensorhub.api.module.IModule;
import org.sensorhub.api.module.ModuleConfig;
import org.sensorhub.api.module.ModuleConfigBase;
import org.sensorhub.api.module.ModuleEvent;
import org.sensorhub.api.module.ModuleEvent.ModuleState;
import org.sensorhub.api.processing.ProcessConfig;
import org.sensorhub.api.security.SecurityModuleConfig;
import org.sensorhub.api.sensor.SensorConfig;
import org.sensorhub.api.service.IHttpServer;
import org.sensorhub.api.service.ServiceConfig;
import org.sensorhub.api.system.ISystemDriver;
import org.sensorhub.api.system.ISystemGroupDriver;
import org.sensorhub.impl.module.ModuleRegistry;
import org.sensorhub.impl.sensor.SensorSystem;
import org.sensorhub.impl.sensor.SensorSystemConfig.SystemMember;
import org.sensorhub.ui.ModuleTypeSelectionPopup.ModuleTypeSelectionCallback;
import org.sensorhub.ui.api.IModuleAdminPanel;
import org.sensorhub.ui.api.UIConstants;
import org.sensorhub.ui.data.MyBeanItem;
import org.sensorhub.utils.ModuleUtils;
import org.sensorhub.utils.MsgUtils;
import org.slf4j.Logger;
import org.vast.ows.OWSUtils;
import com.vaadin.annotations.Push;
import com.vaadin.annotations.Theme;
import com.vaadin.v7.data.Item;
import com.vaadin.v7.data.util.converter.Converter;
import com.vaadin.v7.data.util.converter.ConverterFactory;
import com.vaadin.v7.data.util.converter.DefaultConverterFactory;
import com.vaadin.v7.data.util.converter.StringToIntegerConverter;
import com.vaadin.event.Action;
import com.vaadin.event.Action.Handler;
import com.vaadin.v7.event.ItemClickEvent;
import com.vaadin.v7.event.ItemClickEvent.ItemClickListener;
import com.vaadin.server.FontAwesome;
import com.vaadin.server.Page;
import com.vaadin.server.Resource;
import com.vaadin.server.ThemeResource;
import com.vaadin.server.VaadinRequest;
import com.vaadin.server.VaadinService;
import com.vaadin.server.VaadinServlet;
import com.vaadin.server.VaadinSession;
import com.vaadin.shared.communication.PushMode;
import com.vaadin.shared.ui.ContentMode;
import com.vaadin.shared.ui.ui.Transport;
import com.vaadin.v7.ui.AbstractSelect.ItemDescriptionGenerator;
import com.vaadin.ui.Accordion;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.Component;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.HorizontalSplitPanel;
import com.vaadin.ui.Image;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.ui.Button.ClickListener;
import com.vaadin.ui.TabSheet.SelectedTabChangeEvent;
import com.vaadin.ui.TabSheet.SelectedTabChangeListener;
import com.vaadin.ui.TabSheet.Tab;
import com.vaadin.v7.ui.Table;
import com.vaadin.v7.ui.Table.CellStyleGenerator;
import com.vaadin.v7.ui.Table.ColumnHeaderMode;
import com.vaadin.v7.ui.TreeTable;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.Window;
import com.vaadin.ui.Window.CloseEvent;
import com.vaadin.ui.Window.CloseListener;


@Theme("sensorhub")
@Push(value=PushMode.MANUAL, transport=Transport.LONG_POLLING)
@SuppressWarnings({ "serial", "deprecation" })
public class AdminUI extends com.vaadin.ui.UI implements UIConstants
{
    private transient ResourceBundle resourceBundle;
    private static final String LOG_INIT_MSG = "New connection to admin UI (from ip={}, user={})";
    private static final String LOG_ACTION_MSG = "New UI action: {} (from ip={}, user={})";
        
    private Action ADD_MODULE_ACTION;
    private Action ADD_SUBMODULE_ACTION;
    private Action REMOVE_MODULE_ACTION;
    private Action REMOVE_SUBMODULE_ACTION;
    private Action START_MODULE_ACTION;
    private Action STOP_MODULE_ACTION;
    private Action RESTART_MODULE_ACTION;
    private Action REINIT_MODULE_ACTION;
    private static final Resource LOGO_ICON = new ThemeResource("icons/osh_logo_small.png");
    private static final String STYLE_LOGO = "logo";
    private static final String PROP_STATE = "state";
    private static final String PROP_MODULE_OBJECT = "module";
    
    transient Logger log;
    transient ISensorHub hub;
    transient AdminUIModule adminModule;
    transient ModuleRegistry moduleRegistry;
    transient Subscription moduleEventsSub;
    transient AdminUISecurity securityHandler;
    transient Map<Class<?>, TreeTable> moduleTables = new HashMap<>();
    transient IModule<?> moduleAddedFromUI;
    transient Accordion moduleStack;
    transient VerticalLayout configArea;
    transient IModule<?> visibleModule;
    
    
    @Override
    protected void init(VaadinRequest request)
    {
        Preferences prefs = Preferences.userNodeForPackage(AdminUI.class);
        String savedLanguageTag = prefs.get("userLanguage", null);
        Locale sessionLocale = VaadinSession.getCurrent().getLocale(); // Get current default (e.g. from browser)

        if (savedLanguageTag != null) {
            try {
                Locale loadedLocale = Locale.forLanguageTag(savedLanguageTag);
                if (isSupportedLocale(loadedLocale)) { // Implement isSupportedLocale or use the map
                    VaadinSession.getCurrent().setLocale(loadedLocale);
                    sessionLocale = loadedLocale; // Update for resource bundle use
                }
            } catch (Exception e) {
                // log error
                System.err.println("Error loading saved language preference: " + e.getMessage());
            }
        }
        // Ensure a supported locale is set if current/saved is not
        if (!isSupportedLocale(sessionLocale)) {
           VaadinSession.getCurrent().setLocale(Locale.ENGLISH); // Fallback to English
           sessionLocale = Locale.ENGLISH;
        }
        this.resourceBundle = ResourceBundle.getBundle("org.sensorhub.ui.messages", sessionLocale);
        ADD_MODULE_ACTION = new Action(resourceBundle.getString("adminUI.addModuleAction"), new ThemeResource("icons/module_add.png"));
        ADD_SUBMODULE_ACTION = new Action(resourceBundle.getString("adminUI.addSubmoduleAction"), new ThemeResource("icons/module_add.png"));
        REMOVE_MODULE_ACTION = new Action(resourceBundle.getString("adminUI.removeModuleAction"), new ThemeResource("icons/module_delete.png"));
        REMOVE_SUBMODULE_ACTION = new Action(resourceBundle.getString("adminUI.removeSubmoduleAction"), new ThemeResource("icons/module_delete.png"));
        START_MODULE_ACTION = new Action(resourceBundle.getString("adminUI.startModuleAction"), new ThemeResource("icons/enable.png"));
        STOP_MODULE_ACTION = new Action(resourceBundle.getString("adminUI.stopModuleAction"), new ThemeResource("icons/disable.gif"));
        RESTART_MODULE_ACTION = new Action(resourceBundle.getString("adminUI.restartModuleAction"), new ThemeResource("icons/refresh.gif"));
        REINIT_MODULE_ACTION = new Action(resourceBundle.getString("adminUI.reinitModuleAction"), new ThemeResource("icons/refresh.gif"));
        // retrieve module config
        try
        {
            ServletContext servletContext = VaadinServlet.getCurrent().getServletContext();
            this.adminModule = (AdminUIModule)servletContext.getAttribute(AdminUIModule.SERVLET_PARAM_MODULE);
            this.hub = adminModule.getParentHub();
            this.log = adminModule.getLogger();
            this.moduleRegistry = hub.getModuleRegistry();
            this.securityHandler = adminModule.getSecurityHandler();
        }
        catch (Exception e)
        {
            throw new IllegalStateException("Cannot get UI module configuration", e);
        }
        
        // log request
        logInitRequest(request);
        
        // security check
        if (!securityHandler.hasPermission(securityHandler.admin_access))
        {
            DisplayUtils.showUnauthorizedAccess(securityHandler.admin_access.getErrorMessage());
            securityHandler.clearCurrentUser();
            return;
        }
        
        // register new field converter for integer numbers
        ConverterFactory converterFactory = new DefaultConverterFactory() {
            @Override
            @SuppressWarnings("unchecked")
            protected <Presentation, Model> Converter<Presentation, Model> findConverter(
                    Class<Presentation> presentationType, Class<Model> modelType) {
                // Handle String <-> Integer/Short/Long
                if (presentationType == String.class &&
                   (modelType == Long.class || modelType == Integer.class || modelType == Short.class )) {
                    return (Converter<Presentation, Model>) new StringToIntegerConverter() {
                        @Override
                        protected NumberFormat getFormat(Locale locale) {
                            NumberFormat format = super.getFormat(Locale.US);
                            format.setGroupingUsed(false);
                            return format;
                        }
                    };
                }
                // Let default factory handle the rest
                return super.findConverter(presentationType, modelType);
            }
        };
        VaadinSession.getCurrent().setConverterFactory(converterFactory);
        
        // init main panels
        HorizontalSplitPanel splitPanel = new HorizontalSplitPanel();
        splitPanel.setMinSplitPosition(300.0f, Unit.PIXELS);
        splitPanel.setMaxSplitPosition(30.0f, Unit.PERCENTAGE);
        splitPanel.setSplitPosition(350.0f, Unit.PIXELS);
        setContent(splitPanel);
        
        // build left pane
        VerticalLayout leftPane = new VerticalLayout();
        leftPane.setSizeFull();
        leftPane.setSpacing(false);
        leftPane.setMargin(false);
        
        // header image and title
        Component header = buildHeader();
        leftPane.addComponent(header);
        leftPane.setExpandRatio(header, 0);

        // toolbar
        Component toolbar = buildToolbar();
        leftPane.addComponent(toolbar);
        leftPane.setExpandRatio(toolbar, 0);
        
        // accordion with several sections
        moduleTables.clear();
        final var stack = moduleStack = new Accordion();
        stack.setSizeFull();
        stack.addSelectedTabChangeListener(new SelectedTabChangeListener() {
            @Override
            public void selectedTabChange(SelectedTabChangeEvent event)
            {
                selectStackItem(stack);
            }
        });
        VerticalLayout layout;
        Tab tab;
        
        layout = new VerticalLayout();
        tab = stack.addTab(layout, resourceBundle.getString("adminUI.sensorsTab"));
        //tab.setIcon(ACC_TAB_ICON);
        //tab.setIcon(FontAwesome.VIDEO_CAMERA);
        //tab.setIcon(FontAwesome.STETHOSCOPE);
        tab.setIcon(FontAwesome.RSS);
        buildModuleList(layout, SensorConfig.class);
        
        layout = new VerticalLayout();
        tab = stack.addTab(layout, resourceBundle.getString("adminUI.databasesTab"));
        //tab.setIcon(ACC_TAB_ICON);
        tab.setIcon(FontAwesome.DATABASE);
        buildModuleList(layout, DatabaseConfig.class);
        
        layout = new VerticalLayout();
        tab = stack.addTab(layout, resourceBundle.getString("adminUI.processingTab"));
        //tab.setIcon(ACC_TAB_ICON);
        tab.setIcon(FontAwesome.GEARS);
        buildModuleList(layout, ProcessConfig.class);
        
        layout = new VerticalLayout();
        tab = stack.addTab(layout, resourceBundle.getString("adminUI.servicesTab"));
        //tab.setIcon(ACC_TAB_ICON);
        //tab.setIcon(FontAwesome.CLOUD_DOWNLOAD);
        //tab.setIcon(FontAwesome.CUBES);
        tab.setIcon(FontAwesome.TASKS);
        buildModuleList(layout, ServiceConfig.class);
        
        layout = new VerticalLayout();
        tab = stack.addTab(layout, resourceBundle.getString("adminUI.clientsTab"));
        //tab.setIcon(ACC_TAB_ICON);
        tab.setIcon(FontAwesome.CLOUD_UPLOAD);
        buildModuleList(layout, ClientConfig.class);
        
        layout = new VerticalLayout();
        tab = stack.addTab(layout, resourceBundle.getString("adminUI.networkTab"));
        //tab.setIcon(ACC_TAB_ICON);
        //tab.setIcon(FontAwesome.SIGNAL);
        tab.setIcon(FontAwesome.SITEMAP);
        buildNetworkModuleList(layout);
        
        layout = new VerticalLayout();
        tab = stack.addTab(layout, resourceBundle.getString("adminUI.securityTab"));
        //tab.setIcon(ACC_TAB_ICON);
        tab.setIcon(FontAwesome.LOCK);
        buildModuleList(layout, SecurityModuleConfig.class);
        
        leftPane.addComponent(stack);
        leftPane.setExpandRatio(stack, 1);
        splitPanel.addComponent(leftPane);
        
        // init config area
        configArea = new VerticalLayout();
        configArea.setMargin(true);
        splitPanel.addComponent(configArea);
        
        // select first tab
        stack.setSelectedTab(0);
        selectStackItem(stack);
        
        // register to module registry events
        hub.getEventBus().newSubscription()
            .withTopicID(ModuleRegistry.EVENT_GROUP_ID)
            .consume(this::handleEvent)
            .thenAccept(s -> moduleEventsSub = s);
    }
    
    
    protected void selectStackItem(Accordion stack)
    {
        VerticalLayout tabLayout = (VerticalLayout)stack.getSelectedTab();
        if (tabLayout.getComponentCount() > 0)
        {
            TreeTable table = (TreeTable)tabLayout.getComponent(0);
            Object itemId = table.getValue();
            if (itemId != null)
            {
                IModule<?> module = (IModule<?>)table.getItem(itemId).getItemProperty(PROP_MODULE_OBJECT).getValue();
                selectModule(module, table);
            }
            else
                selectNone(table);
        }
    }
    
    protected Component buildHeader()
    {
        HorizontalLayout header = new HorizontalLayout();
        header.setMargin(false);
        header.setWidth(100.0f, Unit.PERCENTAGE);
        
        // logo
        Image img = new Image(null, LOGO_ICON);
        img.setStyleName(STYLE_LOGO);
        header.addComponent(img);
        header.setExpandRatio(img, 0);
        header.setComponentAlignment(img, Alignment.MIDDLE_LEFT);
        
        // title
        Label title = new Label(resourceBundle.getString("adminUI.title"));
        //title.addStyleName(STYLE_H2);
        title.addStyleName(STYLE_LOGO);
        //title.setWidth(null);
        header.addComponent(title);
        header.setExpandRatio(title, 1);
        header.setComponentAlignment(title, Alignment.MIDDLE_LEFT);

        // Language Selector
        Map<Locale, String> supportedLocales = new LinkedHashMap<>();
        supportedLocales.put(Locale.ENGLISH, "English");
        supportedLocales.put(new Locale("es"), "Español");
        supportedLocales.put(Locale.FRENCH, "Français");
        supportedLocales.put(Locale.GERMAN, "Deutsch");
        supportedLocales.put(new Locale("ru"), "Русский");

        ComboBox<Locale> languageSelector = new ComboBox<>();
        languageSelector.setItems(supportedLocales.keySet());
        languageSelector.setItemCaptionGenerator(locale -> supportedLocales.get(locale));
        languageSelector.setValue(VaadinSession.getCurrent().getLocale());
        languageSelector.setEmptySelectionAllowed(false);
        languageSelector.addValueChangeListener(event -> {
            if (event.getValue() != null && event.isUserOriginated()) {
                VaadinSession.getCurrent().setLocale(event.getValue());
                Preferences localPrefs = Preferences.userNodeForPackage(AdminUI.class);
                localPrefs.put("userLanguage", event.getValue().toLanguageTag());
                try {
                    localPrefs.flush(); // Ensure it's written
                } catch (java.util.prefs.BackingStoreException bse) {
                    // Log or handle the error if preferences can't be flushed
                    bse.printStackTrace();
                }
                Page.getCurrent().reload();
            }
        });
        header.addComponent(languageSelector);
        header.setComponentAlignment(languageSelector, Alignment.MIDDLE_RIGHT);
        header.setExpandRatio(languageSelector, 0);

        // about icon
        Button about = new Button();
        about.addStyleName(STYLE_QUIET);
        about.addStyleName(STYLE_BORDERLESS);
        about.setIcon(FontAwesome.QUESTION_CIRCLE);
        about.addClickListener(new ClickListener() {
            private static final long serialVersionUID = 1L;
            @Override
            public void buttonClick(ClickEvent event)
            {
                String version = ModuleUtils.getModuleInfo(getClass()).getModuleVersion();
                String buildNumber = ModuleUtils.getBuildNumber(getClass());
                Window popup = new Window(MessageFormat.format("<b>{0}</b>", resourceBundle.getString("adminUI.aboutWindowTitle")));
                popup.setIcon(LOGO_ICON);
                popup.setCaptionAsHtml(true);
                popup.setModal(true);
                popup.setClosable(true);
                popup.setResizable(false);
                popup.center();
                VerticalLayout content = new VerticalLayout();
                content.setMargin(true);
                content.setSpacing(true);
                content.addComponent(new Label(resourceBundle.getString("adminUI.aboutMessage")));
                content.addComponent(new Label(resourceBundle.getString("adminUI.licenseMessage"), ContentMode.HTML));
                content.addComponent(new Label(MessageFormat.format("<b>{0}</b> {1}", resourceBundle.getString("adminUI.versionLabel"), (version != null ? version: "?")), ContentMode.HTML));
                content.addComponent(new Label(MessageFormat.format("<b>{0}</b> {1}", resourceBundle.getString("adminUI.buildNumberLabel"), (buildNumber != null ? buildNumber: "?")), ContentMode.HTML));

                // If the config has a friendly node name
                if (adminModule.getConfiguration().deploymentName != null && !adminModule.getConfiguration().deploymentName.isEmpty()) {

                    content.addComponent(new Label(MessageFormat.format("<b>{0}</b> {1}", resourceBundle.getString("adminUI.deploymentNameLabel"), adminModule.getConfiguration().deploymentName), ContentMode.HTML));
                }
                popup.setContent(content);
                addWindow(popup);
            }
        });
        header.addComponent(about);
        header.setExpandRatio(about, 0);
        
        return header;
    }
    
    
    protected Component buildToolbar()
    {
        HorizontalLayout toolbar = new HorizontalLayout();
        toolbar.setWidth(100.0f, Unit.PERCENTAGE);
        toolbar.setSpacing(true);
        toolbar.setStyleName("toolbar");
                
        // shutdown button
        Button shutdownButton = new Button(resourceBundle.getString("adminUI.shutdownButton"));
        shutdownButton.setDescription(resourceBundle.getString("adminUI.shutdownButtonDescription"));
        //shutdownButton.setIcon(DEL_ICON);
        shutdownButton.setIcon(FontAwesome.POWER_OFF);
        shutdownButton.addStyleName(STYLE_SMALL);
        shutdownButton.addStyleName(STYLE_BORDERLESS);
        shutdownButton.setWidth(100.0f, Unit.PERCENTAGE);
        shutdownButton.addClickListener(new Button.ClickListener() {
            @Override
            public void buttonClick(ClickEvent event)
            {
                // security check
                if (!securityHandler.hasPermission(securityHandler.osh_shutdown))
                {
                    DisplayUtils.showUnauthorizedAccess(securityHandler.osh_shutdown.getErrorMessage());
                    return;
                }
                
                final ConfirmDialog popup = new ConfirmDialog(resourceBundle.getString("adminUI.confirmShutdownMessage"));
                popup.addCloseListener(new CloseListener() {
                    @Override
                    public void windowClose(CloseEvent e)
                    {
                        if (popup.isConfirmed())
                        {
                            logAction(securityHandler.osh_shutdown.getName());
                            
                            disconnectFromModuleRegistry();
                            
                            Notification notif = new Notification(
                                    FontAwesome.WARNING.getHtml() + "&nbsp; " + resourceBundle.getString("adminUI.shutdownInitiatedMessage"),
                                    resourceBundle.getString("adminUI.uiWillStopRespondingMessage"),
                                    Notification.Type.ERROR_MESSAGE);
                            notif.setHtmlContentAllowed(true);
                            notif.show(getPage());
                            
                            // disable push mode since it's sometimes throwing an exception
                            // during HTTP server stop process
                            getUI().getPushConfiguration().setPushMode(PushMode.DISABLED);
                            
                            // shutdown in separate thread
                            new Timer().schedule(new TimerTask() {
                                @Override
                                public void run()
                                {
                                    hub.stop(false, true);
                                    System.exit(0);
                                }
                            }, 1000);
                        }
                    }
                });
                
                addWindow(popup);
            }
        });
        toolbar.addComponent(shutdownButton);
        
        // logout button
        Button logoutButton = new Button(resourceBundle.getString("adminUI.logoutButton"));
        logoutButton.setDescription(resourceBundle.getString("adminUI.logoutButtonDescription"));
        logoutButton.setIcon(FontAwesome.SIGN_OUT);
        logoutButton.addStyleName(STYLE_SMALL);
        logoutButton.addStyleName(STYLE_BORDERLESS);
        logoutButton.setWidth(100.0f, Unit.PERCENTAGE);
        logoutButton.addClickListener(new Button.ClickListener() {
            @Override
            public void buttonClick(ClickEvent event)
            {
                final ConfirmDialog popup = new ConfirmDialog(resourceBundle.getString("adminUI.confirmLogoutMessage"));
                popup.addCloseListener(new CloseListener() {
                    @Override
                    public void windowClose(CloseEvent e)
                    {
                        if (popup.isConfirmed())
                        {
                            logAction("logout");
                            disconnectFromModuleRegistry();
                            
                            getUI().getSession().close();
                            var currentUrl = getUI().getPage().getLocation();
                            getUI().getPage().setLocation(currentUrl.resolve("logout"));
                        }
                    }
                });
                
                addWindow(popup);
            }
        });
        toolbar.addComponent(logoutButton);
        
        // apply changes button
        Button saveButton = new Button(resourceBundle.getString("adminUI.saveButton"));
        saveButton.setDescription(resourceBundle.getString("adminUI.saveButtonDescription"));
        saveButton.setIcon(APPLY_ICON);
        saveButton.addStyleName(STYLE_SMALL);
        saveButton.addStyleName(STYLE_BORDERLESS);
        saveButton.setWidth(100.0f, Unit.PERCENTAGE);
        saveButton.addClickListener(new Button.ClickListener() {
            @Override
            public void buttonClick(ClickEvent event)
            {
                // security check
                if (!securityHandler.hasPermission(securityHandler.osh_saveconfig))
                {
                    DisplayUtils.showUnauthorizedAccess(securityHandler.osh_saveconfig.getErrorMessage());
                    return;
                }
                
                final ConfirmDialog popup = new ConfirmDialog(resourceBundle.getString("adminUI.confirmSaveMessage"));
                popup.addCloseListener(new CloseListener() {
                    @Override
                    public void windowClose(CloseEvent e)
                    {
                        if (popup.isConfirmed())
                        {
                            logAction(securityHandler.osh_saveconfig.getName());
                            
                            try
                            {
                                moduleRegistry.saveModulesConfiguration();
                                DisplayUtils.showOperationSuccessful(resourceBundle.getString("adminUI.configurationSavedMessage"));
                            }
                            catch (Exception ex)
                            {
                                String msg = resourceBundle.getString("adminUI.errorSaveFolder");
                                DisplayUtils.showErrorPopup(msg, ex);
                            }
                        }
                    }
                });
                
                addWindow(popup);
            }
        });
        toolbar.addComponent(saveButton);
        
        return toolbar;
    }
    
    
    protected void buildNetworkModuleList(VerticalLayout layout)
    {
        ArrayList<IModule<?>> moduleList = new ArrayList<>();
        
        // add network modules to list
        moduleList.add(moduleRegistry.getModuleByType(IHttpServer.class));
        for (IModule<?> module: moduleRegistry.getLoadedModules())
        {
            ModuleConfig config = module.getConfiguration();
            if (config != null && NetworkConfig.class.isAssignableFrom(config.getClass()))
                moduleList.add(module);
        }
        
        buildModuleList(layout, moduleList, NetworkConfig.class);
    }
    
    
    protected void buildModuleList(VerticalLayout layout, final Class<?> configType)
    {
        ArrayList<IModule<?>> moduleList = new ArrayList<>();
        
        // add federated database
        if (configType == DatabaseConfig.class)
            moduleList.add(new FederatedDbModuleAdapter(getParentHub().getDatabaseRegistry().getFederatedDatabase()));
        
        // add selected modules to list
        for (IModule<?> module: moduleRegistry.getLoadedModules())
        {
            ModuleConfig config = module.getConfiguration();
            if (config != null && configType.isAssignableFrom(config.getClass()))
                moduleList.add(module);
        }
        
        buildModuleList(layout, moduleList, configType);
    }
    
    
    protected void buildModuleList(VerticalLayout layout, List<IModule<?>> moduleList, final Class<?> configType)
    {
        // create table to display module list
        final TreeTable table = new TreeTable();
        table.setSizeFull();
        table.setSelectable(true);
        table.setNullSelectionAllowed(false);
        table.setImmediate(true);
        table.setColumnReorderingAllowed(false);
        table.addContainerProperty(PROP_NAME, String.class, PROP_NAME);
        table.addContainerProperty(PROP_STATE, ModuleState.class, ModuleState.LOADED);
        table.addContainerProperty(PROP_MODULE_OBJECT, IModule.class, null);
        table.setColumnWidth(PROP_STATE, 100);
        table.setColumnHeaderMode(ColumnHeaderMode.HIDDEN);
        layout.addComponent(table);
        moduleTables.put(configType, table);
        
        // add modules info as table items
        for (IModule<?> module: moduleList)
            addModuleToTable(module, table);
        
        // hide module object column!
        table.setVisibleColumns(PROP_NAME, PROP_STATE);
        
        // value converter for state field -> display as text and icon
        table.setConverter(PROP_STATE, new Converter<String, ModuleState>() {
            @Override
            public ModuleState convertToModel(String value, Class<? extends ModuleState> targetType, Locale locale)
            {
                return ModuleState.valueOf(value);
            }

            @Override
            public String convertToPresentation(ModuleState value, Class<? extends String> targetType, Locale locale)
            {
                return value.toString();
            }

            @Override
            public Class<ModuleState> getModelType()
            {
                return ModuleState.class;
            }

            @Override
            public Class<String> getPresentationType()
            {
                return String.class;
            }
        });
        
        table.setCellStyleGenerator(new CellStyleGenerator() {
            @Override
            public String getStyle(Table source, Object itemId, Object propertyId)
            {
                if (propertyId != null && propertyId.equals(PROP_STATE))
                {
                    ModuleState state = (ModuleState)table.getItem(itemId).getItemProperty(propertyId).getValue();
                    IModule<?> module = (IModule<?>)table.getItem(itemId).getItemProperty(PROP_MODULE_OBJECT).getValue();
                    Throwable error = module.getCurrentError();
                    
                    if (error == null)
                    {
                        if (state == ModuleState.STARTED)
                            return "green";
                        else
                            return "red";
                    }
                    else
                    {
                        return "error";
                    }
                }
                
                return null;
            }
        });
        
        table.setItemDescriptionGenerator(new ItemDescriptionGenerator() {                             
            @Override
            public String generateDescription(Component source, Object itemId, Object propertyId) {
                if (propertyId != null && propertyId.equals(PROP_STATE))
                {
                    IModule<?> module = (IModule<?>)table.getItem(itemId).getItemProperty(PROP_MODULE_OBJECT).getValue();
                    Throwable error = module.getCurrentError();
                    if (error != null)
                    {
                        StringBuilder buf = new StringBuilder();
                        buf.append(error.getMessage());
                        if (error.getCause() != null)
                        {
                            buf.append("<br/>");
                            buf.append(error.getCause().getMessage());
                        }
                        return buf.toString();
                    }
                    else
                        return module.getStatusMessage();
                }
                
                return null;
            }
        });
        
        // item click listener to display selected module settings
        table.addItemClickListener(new ItemClickListener()
        {
            @Override
            public void itemClick(ItemClickEvent event)
            {
                try
                {
                    // select and open module configuration
                    IModule<?> module = (IModule<?>)event.getItem().getItemProperty(PROP_MODULE_OBJECT).getValue();
                    selectModule(module, table);
                }
                catch (Exception e)
                {
                    DisplayUtils.showErrorPopup("Unexpected error when selecting module", e);
                }
            }
        });
        
        // context menu
        table.addActionHandler(new Handler() {
            @Override
            public Action[] getActions(Object target, Object sender)
            {
                List<Action> actions = new ArrayList<>(10);
                
                if (target != null)
                {
                    var item = table.getItem(target);
                    var module = (IModule<?>)item.getItemProperty(PROP_MODULE_OBJECT).getValue();
                    var state = (ModuleState)item.getItemProperty(PROP_STATE).getValue();
                    if (state == ModuleState.STARTED)
                        actions.add(RESTART_MODULE_ACTION);
                    else
                        actions.add(START_MODULE_ACTION);
                    
                    actions.add(STOP_MODULE_ACTION);
                    actions.add(REINIT_MODULE_ACTION);
                    
                    actions.add(new Action("-------------------------------"));
                    
                    if (module instanceof SensorSystem)
                        actions.add(ADD_SUBMODULE_ACTION);
                    
                    if (table.getParent(target) != null)
                        actions.add(REMOVE_SUBMODULE_ACTION);
                    else
                        actions.add(REMOVE_MODULE_ACTION);
                    
                    actions.add(ADD_MODULE_ACTION);
                }
                else
                    actions.add(ADD_MODULE_ACTION);
                
                return actions.toArray(new Action[0]);
            }
            
            @Override
            public void handleAction(final Action action, Object sender, Object target)
            {
                // retrieve selected module if any
                final Item selectedItem;
                final IModule<?> selectedModule;
                if (target != null)
                {
                    selectedItem = table.getItem(target);
                    selectedModule = (IModule<?>)selectedItem.getItemProperty(PROP_MODULE_OBJECT).getValue();
                }
                else
                {
                    selectedItem = null;
                    selectedModule = null;
                }
                
                if (action == ADD_MODULE_ACTION)
                {
                    // security check
                    if (!securityHandler.hasPermission(securityHandler.module_add))
                    {
                        DisplayUtils.showUnauthorizedAccess(securityHandler.module_add.getErrorMessage());
                        return;
                    }
                    
                    // show popup to select among available module types
                    ModuleTypeSelectionPopup popup = new ModuleTypeSelectionPopup(configType, new ModuleTypeSelectionCallback() {
                        @Override
                        public void onSelected(ModuleConfigBase config)
                        {
                            try
                            {
                                // log action
                                logAction(action, config.moduleClass);
                                
                                // load module instance
                                IModule<?> module = moduleRegistry.loadModule((ModuleConfig)config);
                                
                                // no need to add module to table here
                                // it will be loaded when the LOADED event is received
                                
                                moduleAddedFromUI = module;
                            }
                            catch (NoClassDefFoundError e)
                            {
                                DisplayUtils.showDependencyError(config.getClass(), e);
                            }
                            catch (Exception e)
                            {
                                DisplayUtils.showErrorPopup(resourceBundle.getString("adminUI.errorLoadModuleMessage"), e);
                            }
                        }
                    });
                    popup.setModal(true);
                    addWindow(popup);
                }
                
                else if (selectedModule != null)
                {
                    // possible actions when a module is selected
                    final String moduleId = (String)target;
                    final String moduleName = (String)selectedItem.getItemProperty(PROP_NAME).getValue();
                    
                    if (action == REMOVE_MODULE_ACTION)
                    {
                        // security check
                        if (!securityHandler.hasPermission(securityHandler.module_remove))
                        {
                            DisplayUtils.showUnauthorizedAccess(securityHandler.module_remove.getErrorMessage());
                            return;
                        }
                        
                        final ConfirmDialog popup = new ConfirmDialog(MessageFormat.format(resourceBundle.getString("adminUI.confirmRemoveModuleMessage"), moduleName));
                        popup.addCloseListener(new CloseListener() {
                            @Override
                            public void windowClose(CloseEvent e)
                            {
                                if (popup.isConfirmed())
                                {
                                    // log action
                                    logAction(action, selectedModule);
                                    
                                    try
                                    {
                                        moduleRegistry.destroyModule(moduleId);
                                        
                                        table.removeItem(moduleId);
                                        selectNone(table);
                                    }
                                    catch (SensorHubException ex)
                                    {
                                        DisplayUtils.showErrorPopup(resourceBundle.getString("adminUI.errorRemoveModuleMessage"), ex);
                                    }
                                }
                            }
                        });
                        
                        addWindow(popup);
                    }
                    
                    else if (action == ADD_SUBMODULE_ACTION)
                    {
                     // security check
                        if (!securityHandler.hasPermission(securityHandler.module_add))
                        {
                            DisplayUtils.showUnauthorizedAccess(securityHandler.module_add.getErrorMessage());
                            return;
                        }
                        
                        // show popup to select among available module types
                        ModuleTypeSelectionPopup popup = new ModuleTypeSelectionPopup(ISystemDriver.class, new ModuleTypeSelectionCallback() {
                            @Override
                            public void onSelected(ModuleConfigBase config)
                            {
                                try
                                {
                                    // log action
                                    logAction(action, config.moduleClass);
                                    
                                    var newMember = new SystemMember();
                                    newMember.config = (ModuleConfig)config;
                                    
                                    var newModule = ((SensorSystem)selectedModule).addSubsystem(newMember);
                                    
                                    var memberId = newModule.getLocalID();
                                    //table.addItem(new Object[] {newModule.getName(), newModule.getCurrentState(), newModule}, memberID);
                                    Item newItem = table.addItem(memberId);
                                    newItem.getItemProperty(PROP_NAME).setValue(newModule.getName());
                                    newItem.getItemProperty(PROP_STATE).setValue(newModule.getCurrentState());
                                    newItem.getItemProperty(PROP_MODULE_OBJECT).setValue(newModule);
                                    table.setParent(memberId, moduleId);
                                    table.setChildrenAllowed(memberId, false);
                                    
                                    selectModule(newModule, table);
                                    moduleAddedFromUI = newModule;
                                }
                                catch (Exception e)
                                {
                                    DisplayUtils.showErrorPopup(resourceBundle.getString("adminUI.errorAddSubmoduleMessage"), e);
                                }
                            }
                        });
                        popup.setModal(true);
                        addWindow(popup);
                    }
                    
                    else if (action == REMOVE_SUBMODULE_ACTION)
                    {
                        // security check
                        if (!securityHandler.hasPermission(securityHandler.module_remove))
                        {
                            DisplayUtils.showUnauthorizedAccess(securityHandler.module_remove.getErrorMessage());
                            return;
                        }
                        
                        final ConfirmDialog popup = new ConfirmDialog(MessageFormat.format(resourceBundle.getString("adminUI.confirmRemoveModuleMessage"), moduleName));
                        popup.addCloseListener(new CloseListener() {
                            @Override
                            public void windowClose(CloseEvent e)
                            {
                                if (popup.isConfirmed())
                                {
                                    // log action
                                    logAction(action, selectedModule);
                                    
                                    try
                                    {
                                        // get parent module
                                        var parentId = table.getParent(moduleId);
                                        var parentModule = (SensorSystem)table.getItem(parentId).getItemProperty(PROP_MODULE_OBJECT).getValue();
                                        parentModule.removeSubSystem(moduleId);
                                        
                                        table.removeItem(moduleId);
                                        selectNone(table);
                                    }
                                    catch (SensorHubException ex)
                                    {
                                        DisplayUtils.showErrorPopup(resourceBundle.getString("adminUI.errorRemoveSubmoduleMessage"), ex);
                                    }
                                }
                            }
                        });
                        
                        addWindow(popup);
                    }
                    
                    else if (action == START_MODULE_ACTION)
                    {
                        // security check
                        if (!securityHandler.hasPermission(securityHandler.module_start))
                        {
                            DisplayUtils.showUnauthorizedAccess(securityHandler.module_start.getErrorMessage());
                            return;
                        }
                        
                        final ConfirmDialog popup = new ConfirmDialog(MessageFormat.format(resourceBundle.getString("adminUI.confirmStartModuleMessage"), moduleName));
                        popup.addCloseListener(new CloseListener() {
                            @Override
                            public void windowClose(CloseEvent e)
                            {
                                if (popup.isConfirmed())
                                {
                                    // log action
                                    logAction(action, selectedModule);
                                    
                                    try 
                                    {
                                        if (selectedModule != null)
                                            moduleRegistry.startModuleAsync(selectedModule);
                                    }
                                    catch (SensorHubException ex)
                                    {
                                        DisplayUtils.showErrorPopup(resourceBundle.getString("adminUI.errorStartModuleMessage"), ex);
                                    }
                                }
                            }
                        });
                        
                        addWindow(popup);
                    }
                    else if (action == STOP_MODULE_ACTION)
                    {
                        // security check
                        if (!securityHandler.hasPermission(securityHandler.module_stop))
                        {
                            DisplayUtils.showUnauthorizedAccess(securityHandler.module_stop.getErrorMessage());
                            return;
                        }
                        
                        final ConfirmDialog popup = new ConfirmDialog(MessageFormat.format(resourceBundle.getString("adminUI.confirmStopModuleMessage"), moduleName));
                        popup.addCloseListener(new CloseListener() {
                            @Override
                            public void windowClose(CloseEvent e)
                            {
                                if (popup.isConfirmed())
                                {
                                    // log action
                                    logAction(action, selectedModule);
                                    
                                    try 
                                    {
                                        if (selectedModule != null)
                                            moduleRegistry.stopModuleAsync(selectedModule);
                                    }
                                    catch (SensorHubException ex)
                                    {
                                        DisplayUtils.showErrorPopup(resourceBundle.getString("adminUI.errorStopModuleMessage"), ex);
                                    }
                                }
                            }
                        });
                        
                        addWindow(popup);
                    }
                    else if (action == RESTART_MODULE_ACTION)
                    {
                        // security check
                        if (!securityHandler.hasPermission(securityHandler.module_restart))
                        {
                            DisplayUtils.showUnauthorizedAccess(securityHandler.module_restart.getErrorMessage());
                            return;
                        }
                        
                        final ConfirmDialog popup = new ConfirmDialog(MessageFormat.format(resourceBundle.getString("adminUI.confirmRestartModuleMessage"), moduleName));
                        popup.addCloseListener(new CloseListener() {
                            @Override
                            public void windowClose(CloseEvent e)
                            {
                                if (popup.isConfirmed())
                                {
                                    // log action
                                    logAction(action, selectedModule);
                                    
                                    try 
                                    {
                                        if (selectedModule != null)
                                            moduleRegistry.restartModuleAsync(selectedModule);
                                    }
                                    catch (SensorHubException ex)
                                    {
                                        DisplayUtils.showErrorPopup(resourceBundle.getString("adminUI.errorRestartModuleMessage"), ex);
                                    }
                                }
                            }
                        });
                        
                        addWindow(popup);
                    }
                    else if (action == REINIT_MODULE_ACTION)
                    {
                        // security check
                        if (!securityHandler.hasPermission(securityHandler.module_init))
                        {
                            DisplayUtils.showUnauthorizedAccess(securityHandler.module_init.getErrorMessage());
                            return;
                        }
                        
                        final ConfirmDialog popup = new ConfirmDialog(MessageFormat.format(resourceBundle.getString("adminUI.confirmReinitModuleMessage"), moduleName));
                        popup.addCloseListener(new CloseListener() {
                            @Override
                            public void windowClose(CloseEvent e)
                            {
                                if (popup.isConfirmed())
                                {
                                    // log action
                                    logAction(action, selectedModule);
                                    
                                    try 
                                    {
                                        if (selectedModule != null)
                                            moduleRegistry.initModuleAsync(selectedModule);
                                    }
                                    catch (SensorHubException ex)
                                    {
                                        DisplayUtils.showErrorPopup(resourceBundle.getString("adminUI.errorReinitModuleMessage"), ex);
                                    }
                                }
                            }
                        });
                        
                        addWindow(popup);
                    }
                }
            }
        });
        
        layout.setSizeFull();
        layout.setMargin(false);
    }
    
    
    protected void addModuleToTable(IModule<?> module, TreeTable table)
    {
        String moduleID = module.getLocalID();
        
        Item newItem = table.addItem(moduleID);
        if (newItem == null) // in case module was already added
            return;
        
        newItem.getItemProperty(PROP_NAME).setValue(module.getName());
        newItem.getItemProperty(PROP_STATE).setValue(module.getCurrentState());
        newItem.getItemProperty(PROP_MODULE_OBJECT).setValue(module);
        
        // add submodules
        if (module instanceof ISystemGroupDriver)
        {
            var subModules = ((ISystemGroupDriver<?>) module).getMembers();
            for (var subModule: subModules.values())
            {
                if (subModule instanceof IModule)
                {
                    IModule<?> member = (IModule<?>)subModule;
                    var memberID = member.getLocalID();
                    table.addItem(new Object[] {member.getName(), member.getCurrentState(), member}, memberID);
                    table.setParent(memberID, moduleID);
                    table.setChildrenAllowed(memberID, false);
                }
            }
        }
        else
        {
            table.setChildrenAllowed(moduleID, false);
        }
        
        // select if module was just added from UI
        if (moduleAddedFromUI == module)
        {
            selectModule(module, table);
            moduleAddedFromUI = null;
        }
        
        // also select if first item added
        else if (table.size() == 1)
            table.select(moduleID);
    }
    
    
    protected void selectModule(IModule<?> module, TreeTable table)
    {
        table.select(module.getLocalID());
        ModuleConfig config = module.getConfiguration().clone();
        MyBeanItem<ModuleConfig> beanItem = new MyBeanItem<>(config);
        openModuleInfo(beanItem, module);
    }
    
    
    protected void selectNone(TreeTable table)
    {
        Object itemId = table.getValue();
        if (itemId != null)
            table.unselect(itemId);
        configArea.removeAllComponents();
    }
        
    
    protected void openModuleInfo(MyBeanItem<ModuleConfig> beanItem, IModule<?> module)
    {
        // do nothing if config area hasn't been created yet
        if (configArea == null)
            return;
        
        configArea.removeAllComponents();
        
        // get panel for this config object
        Class<?> configClass = beanItem.getBean().getClass();
        IModuleAdminPanel<IModule<?>> panel = adminModule.generatePanel(configClass);
        panel.build(beanItem, module);
        
        // generate module admin panel
        configArea.addComponent(panel);
        visibleModule = module;
    }


    public void handleEvent(final org.sensorhub.api.event.Event e)
    {
        if (e instanceof ModuleEvent)
        {
            final IModule<?> module = (IModule<?>)e.getSource();
            final ModuleConfig config = module.getConfiguration();
            
            // find table and item corresponding to module
            TreeTable table = null;
            Item item = null;
            for (var t: moduleTables.values())
            {
                item = t.getItem(module.getLocalID());
                if (item != null)
                {
                    table = t;
                    break;
                }
            }
            
            // if no table was found, perhaps module was just loaded
            // so try to find table to add it to
            if (table == null)
            {
                for (Class<?> configClass: moduleTables.keySet())
                {
                    if (configClass.isAssignableFrom(config.getClass()))
                    {
                        table = moduleTables.get(configClass);
                        break;
                    }
                }
            }
            
            // update table according to event type
            final TreeTable foundTable = table;
            final Item foundItem = item;
            switch (((ModuleEvent)e).getType())
            {
                case LOADED:
                    if (foundTable != null)
                    {
                        access(new Runnable() {
                            @Override
                            public void run()
                            {
                                // add module to table
                                if (foundTable != null)
                                {
                                    addModuleToTable(module, foundTable);
                                    push();
                                }
                            }
                        });
                    }
                    break;
                    
                case DELETED:
                    if (foundTable != null)
                    {
                        access(new Runnable() {
                            @Override
                            public void run()
                            {
                                // add module to table
                                if (foundTable != null)
                                {
                                    var wasSelected = foundTable.isSelected(module.getLocalID());
                                    foundTable.removeItem(module.getLocalID());
                                    if (wasSelected)
                                        selectNone(foundTable);
                                    
                                    // cleanup error state
                                    setModuleErrorState(foundTable, module, false);
                                    
                                    push();
                                }
                            }
                        });
                    }
                    break;
                    
                case CONFIG_CHANGED:
                    if (foundItem != null)
                    {
                        access(new Runnable() {
                            @Override
                            public void run()
                            {
                                // update module name
                                foundItem.getItemProperty(PROP_NAME).setValue(config.name);
                                push();
                            }
                        });
                    }
                    break;
                    
                case STATE_CHANGED:
                case ERROR:
                    if (foundItem != null)
                    {
                        access(new Runnable() {
                            @Override
                            public void run()
                            {
                                // update module state
                                ModuleState state = ((IModule<?>)e.getSource()).getCurrentState();
                                if (foundItem != null)
                                    foundItem.getItemProperty(PROP_STATE).setValue(state);
                                
                                // set/clear error flags on accordion headers
                                if (foundTable != null)
                                    setModuleErrorState(foundTable, module, module.getCurrentError() != null);
                                
                                // update config panel if currently visible
                                if (module == visibleModule)
                                    selectModule(module, foundTable);
                                
                                push();
                            }
                        });
                    }
                    break;
                    
                default:
            }
        }
    }
    
    
    protected void setModuleErrorState(TreeTable moduleTable, IModule<?> module, boolean hasError)
    {
        var tab = moduleStack.getTab(moduleTable.getParent());
        if (tab != null)
        {
            var errors = (ModuleErrors)tab.getComponentError();
            
            if (hasError)
            {
                if (errors == null)
                    errors = new ModuleErrors();
                
                errors.setModuleErrorState(module, true);
                tab.setComponentError(errors);
            }
            else
            {
                if (errors != null)
                {
                    if (!errors.setModuleErrorState(module, false))
                        tab.setComponentError(null);
                }
            }
        }
    }
    
    
    protected void logInitRequest(VaadinRequest req)
    {
        if (log.isInfoEnabled())
        {
            String ip = req.getRemoteAddr();
            String user = req.getRemoteUser() != null ? req.getRemoteUser() : OWSUtils.ANONYMOUS_USER;
            log.info(LOG_INIT_MSG, ip, user);
        }
    }
    
    
    protected void logAction(String action)
    {
        if (log.isInfoEnabled())
        {
            VaadinRequest req = VaadinService.getCurrentRequest();
            String ip = req.getRemoteAddr();
            String user = (req.getRemoteUser() != null) ? req.getRemoteUser() : OWSUtils.ANONYMOUS_USER;
            log.info(LOG_ACTION_MSG, action, ip, user);
        }
    }
    
    
    protected void logAction(String action, String item)
    {
        if (log.isInfoEnabled())
            logAction(action + " " + item);
    }
    
    
    protected void logAction(String action, IModule<?> module)
    {
        if (log.isInfoEnabled())
            logAction(action, MsgUtils.moduleString(module));
    }
    
    
    protected void logAction(Action action, String item)
    {
        logAction(action.getCaption(), item);
    }
    
    
    protected void logAction(Action action, IModule<?> module)
    {
        logAction(action.getCaption(), module);
    }
    
    
    protected void disconnectFromModuleRegistry()
    {
        // unregister from module registry events
        if (moduleEventsSub != null)
            moduleEventsSub.cancel();
    }


    @Override
    public void detach()
    {
        disconnectFromModuleRegistry();
        super.detach();
    }
    
    
    public ISensorHub getParentHub()
    {
        return hub;
    }
        
    
    public AdminUIModule getParentModule()
    {
        return adminModule;
    }
    
    
    public AdminUISecurity getSecurityHandler()
    {
        return securityHandler;
    }
    
    
    public Logger getOshLogger()
    {
        return adminModule.getLogger();
    }

    private boolean isSupportedLocale(Locale locale) {
        if (locale == null) return false;
        // Reuse the map defined in buildHeader, or redefine supported locales here
        // For simplicity, check against known languages
        return locale.getLanguage().equals(Locale.ENGLISH.getLanguage()) ||
               locale.getLanguage().equals("es") ||
               locale.getLanguage().equals(Locale.FRENCH.getLanguage()) ||
               locale.getLanguage().equals(Locale.GERMAN.getLanguage()) ||
               locale.getLanguage().equals("ru"); // Added Russian
    }
}
