package org.codelibs.elasticsearch.langfield;

import static org.elasticsearch.common.collect.Lists.newArrayList;

import java.util.Collection;

import org.codelibs.elasticsearch.langfield.module.LangFieldIndexModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;

public class LangFieldPlugin extends AbstractPlugin {
    @Override
    public String name() {
        return "LangFieldPlugin";
    }

    @Override
    public String description() {
        return "This plugin provides \"langfield\" type.";
    }

    @Override
    public Collection<Class<? extends Module>> indexModules() {
        final Collection<Class<? extends Module>> modules = newArrayList();
        modules.add(LangFieldIndexModule.class);
        return modules;
    }
}
