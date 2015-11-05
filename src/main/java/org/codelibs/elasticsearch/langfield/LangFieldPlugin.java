package org.codelibs.elasticsearch.langfield;

import java.util.Collection;

import org.codelibs.elasticsearch.langfield.module.LangFieldIndexModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;

import com.google.common.collect.Lists;

public class LangFieldPlugin extends Plugin {
    @Override
    public String name() {
        return "langfield";
    }

    @Override
    public String description() {
        return "This plugin provides langfield type.";
    }

    @Override
    public Collection<Module> indexModules(Settings indexSettings) {
        final Collection<Module> modules = Lists.newArrayList();
        modules.add(new LangFieldIndexModule());
        return modules;
    }
}
