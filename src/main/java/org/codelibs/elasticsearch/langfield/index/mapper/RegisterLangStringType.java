package org.codelibs.elasticsearch.langfield.index.mapper;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.settings.IndexSettingsService;

public class RegisterLangStringType extends AbstractIndexComponent {

    @Inject
    public RegisterLangStringType(final Index index,
            final IndexSettingsService indexSettingsService,
            final MapperService mapperService) {
        super(index, indexSettingsService.getSettings());

        mapperService.documentMapperParser().putTypeParser("langstring",
                new LangStringFieldMapper.TypeParser());
    }

}
