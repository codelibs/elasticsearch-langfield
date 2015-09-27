package org.codelibs.elasticsearch.langfield.index.mapper;

import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseMultiField;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.BytesRef;
import org.codelibs.elasticsearch.langfield.detect.LangDetector;
import org.codelibs.elasticsearch.langfield.detect.LangDetectorFactory;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeContext;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.similarity.SimilarityProvider;

public class LangStringFieldMapper extends AbstractFieldMapper<String>
        implements AllFieldMapper.IncludeInAll {

    public static final String CONTENT_TYPE = "langstring";

    private static final String SEPARATOR_SETTING_KEY = "separator";

    private static final String LANG_SETTING_KEY = "lang";

    private static final String[] SUPPORTED_LANGUAGES = new String[] { "ar",
            "bg", "bn", "ca", "cs", "da", "de", "el", "en", "es", "et", "fa",
            "fi", "fr", "gu", "he", "hi", "hr", "hu", "id", "it", "ja", "ko",
            "lt", "lv", "mk", "ml", "nl", "no", "pa", "pl", "pt", "ro", "ru",
            "si", "sq", "sv", "ta", "te", "th", "tl", "tr", "uk", "ur", "vi",
            "zh-cn", "zh-tw" };

    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final FieldType FIELD_TYPE = new FieldType(
                AbstractFieldMapper.Defaults.FIELD_TYPE);

        static {
            FIELD_TYPE.freeze();
        }

        // NOTE, when adding defaults here, make sure you add them in the builder
        public static final String NULL_VALUE = null;

        public static final int POSITION_OFFSET_GAP = 0;

        public static final int IGNORE_ABOVE = -1;
    }

    public static class Builder extends
            AbstractFieldMapper.Builder<Builder, LangStringFieldMapper> {

        protected String nullValue = Defaults.NULL_VALUE;

        protected int positionOffsetGap = Defaults.POSITION_OFFSET_GAP;

        protected NamedAnalyzer searchQuotedAnalyzer;

        protected int ignoreAbove = Defaults.IGNORE_ABOVE;

        public Builder(String name) {
            super(name, new FieldType(Defaults.FIELD_TYPE));
            builder = this;
        }

        public Builder nullValue(String nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        @Override
        public Builder searchAnalyzer(NamedAnalyzer searchAnalyzer) {
            super.searchAnalyzer(searchAnalyzer);
            if (searchQuotedAnalyzer == null) {
                searchQuotedAnalyzer = searchAnalyzer;
            }
            return this;
        }

        public Builder positionOffsetGap(int positionOffsetGap) {
            this.positionOffsetGap = positionOffsetGap;
            return this;
        }

        public Builder searchQuotedAnalyzer(NamedAnalyzer analyzer) {
            this.searchQuotedAnalyzer = analyzer;
            return builder;
        }

        public Builder ignoreAbove(int ignoreAbove) {
            this.ignoreAbove = ignoreAbove;
            return this;
        }

        @Override
        public LangStringFieldMapper build(BuilderContext context) {
            if (positionOffsetGap > 0) {
                indexAnalyzer = new NamedAnalyzer(indexAnalyzer,
                        positionOffsetGap);
                searchAnalyzer = new NamedAnalyzer(searchAnalyzer,
                        positionOffsetGap);
                searchQuotedAnalyzer = new NamedAnalyzer(searchQuotedAnalyzer,
                        positionOffsetGap);
            }
            // if the field is not analyzed, then by default, we should omit norms and have docs only
            // index options, as probably what the user really wants
            // if they are set explicitly, we will use those values
            // we also change the values on the default field type so that toXContent emits what
            // differs from the defaults
            FieldType defaultFieldType = new FieldType(Defaults.FIELD_TYPE);
            if (fieldType.indexed() && !fieldType.tokenized()) {
                defaultFieldType.setOmitNorms(true);
                defaultFieldType.setIndexOptions(IndexOptions.DOCS_ONLY);
                if (!omitNormsSet && boost == Defaults.BOOST) {
                    fieldType.setOmitNorms(true);
                }
                if (!indexOptionsSet) {
                    fieldType.setIndexOptions(IndexOptions.DOCS_ONLY);
                }
            }
            defaultFieldType.freeze();
            LangStringFieldMapper fieldMapper = new LangStringFieldMapper(
                    buildNames(context), boost, fieldType, defaultFieldType,
                    docValues, nullValue, indexAnalyzer, searchAnalyzer,
                    searchQuotedAnalyzer, positionOffsetGap, ignoreAbove,
                    postingsProvider, docValuesProvider, similarity,
                    normsLoading, fieldDataSettings, context.indexSettings(),
                    multiFieldsBuilder.build(this, context), copyTo);
            fieldMapper.includeInAll(includeInAll);
            return fieldMapper;
        }

        public NamedAnalyzer indexAnalyzer() {
            return indexAnalyzer;
        }

        public NamedAnalyzer searchAnalyzer() {
            return searchAnalyzer;
        }
    }

    public static LangStringFieldMapper.Builder langStringField(String name) {
        return new LangStringFieldMapper.Builder(name);
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node,
                ParserContext parserContext) throws MapperParsingException {
            LangStringFieldMapper.Builder builder = langStringField(name);
            parseField(builder, name, node, parserContext);
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String propName = Strings.toUnderscoreCase(entry.getKey());
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    if (propNode == null) {
                        throw new MapperParsingException(
                                "Property [null_value] cannot be null.");
                    }
                    builder.nullValue(propNode.toString());
                } else if (propName.equals("search_quote_analyzer")) {
                    NamedAnalyzer analyzer = parserContext.analysisService()
                            .analyzer(propNode.toString());
                    if (analyzer == null) {
                        throw new MapperParsingException("Analyzer ["
                                + propNode.toString()
                                + "] not found for field [" + name + "]");
                    }
                    builder.searchQuotedAnalyzer(analyzer);
                } else if (propName.equals("position_offset_gap")) {
                    builder.positionOffsetGap(
                            XContentMapValues.nodeIntegerValue(propNode, -1));
                    // we need to update to actual analyzers if they are not set in this case...
                    // so we can inject the position offset gap...
                    if (builder.indexAnalyzer() == null) {
                        builder.indexAnalyzer(parserContext.analysisService()
                                .defaultIndexAnalyzer());
                    }
                    if (builder.searchAnalyzer() == null) {
                        builder.searchAnalyzer(parserContext.analysisService()
                                .defaultSearchAnalyzer());
                    }
                    if (builder.searchQuotedAnalyzer == null) {
                        builder.searchQuotedAnalyzer = parserContext
                                .analysisService().defaultSearchQuoteAnalyzer();
                    }
                } else if (propName.equals("ignore_above")) {
                    builder.ignoreAbove(
                            XContentMapValues.nodeIntegerValue(propNode, -1));
                } else {
                    parseMultiField(builder, name, parserContext, propName,
                            propNode);
                }
            }
            return builder;
        }
    }

    private String nullValue;

    private Boolean includeInAll;

    private int positionOffsetGap;

    private NamedAnalyzer searchQuotedAnalyzer;

    private int ignoreAbove;

    private final FieldType defaultFieldType;

    private final LangDetectorFactory langDetectorFactory;

    protected LangStringFieldMapper(Names names, float boost,
            FieldType fieldType, FieldType defaultFieldType, Boolean docValues,
            String nullValue, NamedAnalyzer indexAnalyzer,
            NamedAnalyzer searchAnalyzer, NamedAnalyzer searchQuotedAnalyzer,
            int positionOffsetGap, int ignoreAbove,
            PostingsFormatProvider postingsFormat,
            DocValuesFormatProvider docValuesFormat,
            SimilarityProvider similarity, Loading normsLoading,
            @Nullable Settings fieldDataSettings, Settings indexSettings,
            MultiFields multiFields, CopyTo copyTo) {
        super(names, boost, fieldType, docValues, indexAnalyzer, searchAnalyzer,
                postingsFormat, docValuesFormat, similarity, normsLoading,
                fieldDataSettings, indexSettings, multiFields, copyTo);
        if (fieldType.tokenized() && fieldType.indexed() && hasDocValues()) {
            throw new MapperParsingException("Field [" + names.fullName()
                    + "] cannot be analyzed and have doc values");
        }
        this.defaultFieldType = defaultFieldType;
        this.nullValue = nullValue;
        this.positionOffsetGap = positionOffsetGap;
        this.searchQuotedAnalyzer = searchQuotedAnalyzer != null
                ? searchQuotedAnalyzer : this.searchAnalyzer;
        this.ignoreAbove = ignoreAbove;

        langDetectorFactory = LangDetectorFactory.create(
                fieldDataType.getSettings().getAsArray(LANG_SETTING_KEY));
    }

    @Override
    public FieldType defaultFieldType() {
        return defaultFieldType;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return new FieldDataType("string",
                ImmutableSettings.builder().put(SEPARATOR_SETTING_KEY, "_")
                        .putArray(LANG_SETTING_KEY, SUPPORTED_LANGUAGES));
    }

    @Override
    public void includeInAll(Boolean includeInAll) {
        if (includeInAll != null) {
            this.includeInAll = includeInAll;
        }
    }

    @Override
    public void includeInAllIfNotSet(Boolean includeInAll) {
        if (includeInAll != null && this.includeInAll == null) {
            this.includeInAll = includeInAll;
        }
    }

    @Override
    public void unsetIncludeInAll() {
        includeInAll = null;
    }

    @Override
    public String value(Object value) {
        if (value == null) {
            return null;
        }
        return value.toString();
    }

    @Override
    protected boolean customBoost() {
        return true;
    }

    public int getPositionOffsetGap() {
        return this.positionOffsetGap;
    }

    @Override
    public Analyzer searchQuoteAnalyzer() {
        return this.searchQuotedAnalyzer;
    }

    @Override
    public Filter nullValueFilter() {
        if (nullValue == null) {
            return null;
        }
        return termFilter(nullValue, null);
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields)
            throws IOException {
        ValueAndBoost valueAndBoost = parseCreateFieldForString(context,
                nullValue, boost);
        if (valueAndBoost.value() == null) {
            return;
        }
        if (ignoreAbove > 0 && valueAndBoost.value().length() > ignoreAbove) {
            return;
        }
        if (context.includeInAll(includeInAll, this)) {
            context.allEntries().addText(names.fullName(),
                    valueAndBoost.value(), valueAndBoost.boost());
        }

        if (fieldType.indexed() || fieldType.stored()) {
            Field field = new Field(names.indexName(), valueAndBoost.value(),
                    fieldType);
            field.setBoost(valueAndBoost.boost());
            fields.add(field);
        }
        if (hasDocValues()) {
            fields.add(new SortedSetDocValuesField(names.indexName(),
                    new BytesRef(valueAndBoost.value())));
        }
        if (fields.isEmpty()) {
            context.ignoredValue(names.indexName(), valueAndBoost.value());
        }

        final LangDetector langDetector = langDetectorFactory.getLangDetector();
        langDetector.append(valueAndBoost.value());
        final String lang = langDetector.detect();
        if (!LangDetector.UNKNOWN_LANG.equals(lang)) {
            final String separator = fieldDataType.getSettings()
                    .get(SEPARATOR_SETTING_KEY);
            parseLangCopyField(names.indexName() + separator + lang, context);
        }
    }

    /**
     * Parse a field as though it were a string.
     * @param context parse context used during parsing
     * @param nullValue value to use for null
     * @param defaultBoost default boost value returned unless overwritten in the field
     * @return the parsed field and the boost either parsed or defaulted
     * @throws IOException if thrown while parsing
     */
    public static ValueAndBoost parseCreateFieldForString(ParseContext context,
            String nullValue, float defaultBoost) throws IOException {
        if (context.externalValueSet()) {
            return new ValueAndBoost((String) context.externalValue(),
                    defaultBoost);
        }
        XContentParser parser = context.parser();
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            return new ValueAndBoost(nullValue, defaultBoost);
        }
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            XContentParser.Token token;
            String currentFieldName = null;
            String value = nullValue;
            float boost = defaultBoost;
            while ((token = parser
                    .nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else {
                    if ("value".equals(currentFieldName)
                            || "_value".equals(currentFieldName)) {
                        value = parser.textOrNull();
                    } else if ("boost".equals(currentFieldName)
                            || "_boost".equals(currentFieldName)) {
                        boost = parser.floatValue();
                    } else {
                        throw new ElasticsearchIllegalArgumentException(
                                "unknown property [" + currentFieldName + "]");
                    }
                }
            }
            return new ValueAndBoost(value, boost);
        }
        return new ValueAndBoost(parser.textOrNull(), defaultBoost);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext)
            throws MergeMappingException {
        super.merge(mergeWith, mergeContext);
        if (!this.getClass().equals(mergeWith.getClass())) {
            return;
        }
        if (!mergeContext.mergeFlags().simulate()) {
            this.includeInAll = ((LangStringFieldMapper) mergeWith).includeInAll;
            this.nullValue = ((LangStringFieldMapper) mergeWith).nullValue;
            this.ignoreAbove = ((LangStringFieldMapper) mergeWith).ignoreAbove;
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder,
            boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults || nullValue != null) {
            builder.field("null_value", nullValue);
        }
        if (includeInAll != null) {
            builder.field("include_in_all", includeInAll);
        } else if (includeDefaults) {
            builder.field("include_in_all", false);
        }

        if (includeDefaults
                || positionOffsetGap != Defaults.POSITION_OFFSET_GAP) {
            builder.field("position_offset_gap", positionOffsetGap);
        }
        if (searchQuotedAnalyzer != null
                && !searchQuotedAnalyzer.name().equals(searchAnalyzer.name())) {
            builder.field("search_quote_analyzer", searchQuotedAnalyzer.name());
        } else if (includeDefaults) {
            if (searchQuotedAnalyzer == null) {
                builder.field("search_quote_analyzer", "default");
            } else {
                builder.field("search_quote_analyzer",
                        searchQuotedAnalyzer.name());
            }
        }
        if (includeDefaults || ignoreAbove != Defaults.IGNORE_ABOVE) {
            builder.field("ignore_above", ignoreAbove);
        }
    }

    /**
     * Parsed value and boost to be returned from {@link #parseCreateFieldForString}.
     */
    public static class ValueAndBoost {
        private final String value;

        private final float boost;

        public ValueAndBoost(String value, float boost) {
            this.value = value;
            this.boost = boost;
        }

        /**
         * Value of string field.
         * @return value of string field
         */
        public String value() {
            return value;
        }

        /**
         * Boost either parsed from the document or defaulted.
         * @return boost either parsed from the document or defaulted
         */
        public float boost() {
            return boost;
        }
    }

    /**
     * Creates an copy of the current field with given field name and boost
     * 
     * @param field field name
     * @param context parsed content
     * @throws IOException content parsing error
     */
    public void parseLangCopyField(final String field, ParseContext context)
            throws IOException {
        final FieldMappers mappers = context.docMapper().mappers()
                .indexName(field);
        if (mappers != null && !mappers.isEmpty()) {
            mappers.mapper().parse(context);
        } else {
            // The path of the dest field might be completely different from the current one so we need to reset it
            context = context.overridePath(new ContentPath(0));

            final int posDot = field.lastIndexOf('.');
            if (posDot > 0) {
                // Compound name
                final String objectPath = field.substring(0, posDot);
                final String fieldPath = field.substring(posDot + 1);
                final ObjectMapper mapper = context.docMapper().objectMappers()
                        .get(objectPath);
                if (mapper == null) {
                    //TODO: Create an object dynamically?
                    throw new MapperParsingException(
                            "attempt to copy value to non-existing object ["
                                    + field + "]");
                }

                context.path().add(objectPath);

                // We might be in dynamically created field already, so need to clean withinNewMapper flag
                // and then restore it, so we wouldn't miss new mappers created from copy_to fields
                final boolean origWithinNewMapper = context.isWithinNewMapper();
                context.clearWithinNewMapper();

                try {
                    mapper.parseDynamicValue(context, fieldPath,
                            context.parser().currentToken());
                } finally {
                    if (origWithinNewMapper) {
                        context.setWithinNewMapper();
                    } else {
                        context.clearWithinNewMapper();
                    }
                }

            } else {
                // We might be in dynamically created field already, so need to clean withinNewMapper flag
                // and then restore it, so we wouldn't miss new mappers created from copy_to fields
                final boolean origWithinNewMapper = context.isWithinNewMapper();
                context.clearWithinNewMapper();
                try {
                    context.docMapper().root().parseDynamicValue(context, field,
                            context.parser().currentToken());
                } finally {
                    if (origWithinNewMapper) {
                        context.setWithinNewMapper();
                    } else {
                        context.clearWithinNewMapper();
                    }
                }

            }
        }
    }

}