package org.codelibs.elasticsearch.langfield.module;

import org.codelibs.elasticsearch.langfield.index.mapper.RegisterLangStringType;
import org.elasticsearch.common.inject.AbstractModule;

public class LangFieldIndexModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(RegisterLangStringType.class).asEagerSingleton();
    }
}
