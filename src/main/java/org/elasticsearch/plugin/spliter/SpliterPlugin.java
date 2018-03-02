package org.elasticsearch.plugin.spliter;

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Collections;

/**
 * @author xingtianyu(code4j) Created on 2018-2-25.
 */
public class SpliterPlugin extends Plugin {

    public SpliterPlugin() {
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public String description() {
        return null;
    }

    @Override
    public Collection<Module> nodeModules() {
        return Collections.singletonList(new SplitModule());
    }
}
