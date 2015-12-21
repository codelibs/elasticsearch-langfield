package org.codelibs.elasticsearch.langfield;

import org.codelibs.elasticsearch.langfield.index.mapper.LangStringFieldMapper;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.plugins.Plugin;

public class LangFieldPlugin extends Plugin {
    @Override
    public String name() {
        return "langfield";
    }

    @Override
    public String description() {
        return "This plugin provides langfield type.";
    }

    public void onModule(IndicesModule indicesModule) {
        indicesModule.registerMapper("langstring",
                new LangStringFieldMapper.TypeParser());
    }
}
