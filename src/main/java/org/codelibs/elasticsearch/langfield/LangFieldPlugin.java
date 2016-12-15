package org.codelibs.elasticsearch.langfield;

import java.util.Collections;
import java.util.Map;

import org.codelibs.elasticsearch.langfield.index.mapper.LangStringFieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;

public class LangFieldPlugin extends Plugin implements MapperPlugin {

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Collections.<String, Mapper.TypeParser> singletonMap(LangStringFieldMapper.CONTENT_TYPE,
                new LangStringFieldMapper.TypeParser());
    }
}
