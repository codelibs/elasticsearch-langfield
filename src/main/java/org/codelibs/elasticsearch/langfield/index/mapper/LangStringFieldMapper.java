package org.codelibs.elasticsearch.langfield.index.mapper;

import static org.apache.lucene.index.IndexOptions.NONE;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseMultiField;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.codelibs.elasticsearch.langfield.detect.LangDetector;
import org.codelibs.elasticsearch.langfield.detect.LangDetectorFactory;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.MergeResult;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;

public class LangStringFieldMapper extends FieldMapper
        implements AllFieldMapper.IncludeInAll {

    public static final String CONTENT_TYPE = "langstring";

    private static final int POSITION_INCREMENT_GAP_USE_ANALYZER = -1;

    private static final String SEPARATOR_SETTING_KEY = "separator";

    private static final String LANG_SETTING_KEY = "lang";

    private static final String[] SUPPORTED_LANGUAGES = new String[] { "ar",
            "bg", "bn", "ca", "cs", "da", "de", "el", "en", "es", "et", "fa",
            "fi", "fr", "gu", "he", "hi", "hr", "hu", "id", "it", "ja", "ko",
            "lt", "lv", "mk", "ml", "nl", "no", "pa", "pl", "pt", "ro", "ru",
            "si", "sq", "sv", "ta", "te", "th", "tl", "tr", "uk", "ur", "vi",
            "zh-cn", "zh-tw" };

    private static final String FIELD_SEPARATOR = "_";

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new LangStringFieldType();

        static {
            FIELD_TYPE.freeze();
        }

        // NOTE, when adding defaults here, make sure you add them in the builder
        public static final String NULL_VALUE = null;

        /**
         * Post 2.0 default for position_increment_gap. Set to 100 so that
         * phrase queries of reasonably high slop will not match across field
         * values.
         */
        public static final int POSITION_INCREMENT_GAP = 100;

        public static final int POSITION_INCREMENT_GAP_PRE_2_0 = 0;

        public static final int IGNORE_ABOVE = -1;

        /**
         * The default position_increment_gap for a particular version of Elasticsearch.
         */
        public static int positionIncrementGap(Version version) {
            if (version.before(Version.V_2_0_0_beta1)) {
                return POSITION_INCREMENT_GAP_PRE_2_0;
            }
            return POSITION_INCREMENT_GAP;
        }
    }

    public static class Builder
            extends FieldMapper.Builder<Builder, LangStringFieldMapper> {

        protected String nullValue = Defaults.NULL_VALUE;

        /**
         * The distance between tokens from different values in the same field.
         * POSITION_INCREMENT_GAP_USE_ANALYZER means default to the analyzer's
         * setting which in turn defaults to Defaults.POSITION_INCREMENT_GAP.
         */
        protected int positionIncrementGap = POSITION_INCREMENT_GAP_USE_ANALYZER;

        protected int ignoreAbove = Defaults.IGNORE_ABOVE;

        protected String fieldSeparator = FIELD_SEPARATOR;

        protected String[] supportedLanguages = SUPPORTED_LANGUAGES;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public Builder searchAnalyzer(NamedAnalyzer searchAnalyzer) {
            super.searchAnalyzer(searchAnalyzer);
            return this;
        }

        public Builder positionIncrementGap(int positionIncrementGap) {
            this.positionIncrementGap = positionIncrementGap;
            return this;
        }

        public Builder searchQuotedAnalyzer(NamedAnalyzer analyzer) {
            this.fieldType.setSearchQuoteAnalyzer(analyzer);
            return builder;
        }

        public Builder ignoreAbove(int ignoreAbove) {
            this.ignoreAbove = ignoreAbove;
            return this;
        }

        public Builder fieldSeparator(String fieldSeparator) {
            this.fieldSeparator = fieldSeparator;
            return this;
        }

        public Builder supportedLanguages(String[] supportedLanguages) {
            this.supportedLanguages = supportedLanguages;
            return this;
        }

        @Override
        public LangStringFieldMapper build(BuilderContext context) {
            if (positionIncrementGap != POSITION_INCREMENT_GAP_USE_ANALYZER) {
                fieldType.setIndexAnalyzer(new NamedAnalyzer(
                        fieldType.indexAnalyzer(), positionIncrementGap));
                fieldType.setSearchAnalyzer(new NamedAnalyzer(
                        fieldType.searchAnalyzer(), positionIncrementGap));
                fieldType.setSearchQuoteAnalyzer(new NamedAnalyzer(
                        fieldType.searchQuoteAnalyzer(), positionIncrementGap));
            }
            // if the field is not analyzed, then by default, we should omit norms and have docs only
            // index options, as probably what the user really wants
            // if they are set explicitly, we will use those values
            // we also change the values on the default field type so that toXContent emits what
            // differs from the defaults
            if (fieldType.indexOptions() != IndexOptions.NONE
                    && !fieldType.tokenized()) {
                defaultFieldType.setOmitNorms(true);
                defaultFieldType.setIndexOptions(IndexOptions.DOCS);
                if (!omitNormsSet && fieldType.boost() == 1.0f) {
                    fieldType.setOmitNorms(true);
                }
                if (!indexOptionsSet) {
                    fieldType.setIndexOptions(IndexOptions.DOCS);
                }
            }
            setupFieldType(context);
            LangStringFieldMapper fieldMapper = new LangStringFieldMapper(name,
                    fieldType, defaultFieldType, positionIncrementGap,
                    ignoreAbove, fieldSeparator, supportedLanguages,
                    context.indexSettings(),
                    multiFieldsBuilder.build(this, context), copyTo);
            fieldMapper.includeInAll(includeInAll);
            return fieldMapper;
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
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet()
                    .iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = Strings.toUnderscoreCase(entry.getKey());
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    if (propNode == null) {
                        throw new MapperParsingException(
                                "Property [null_value] cannot be null.");
                    }
                    builder.nullValue(propNode.toString());
                    iterator.remove();
                } else if (propName.equals("search_quote_analyzer")) {
                    NamedAnalyzer analyzer = parserContext.analysisService()
                            .analyzer(propNode.toString());
                    if (analyzer == null) {
                        throw new MapperParsingException("Analyzer ["
                                + propNode.toString()
                                + "] not found for field [" + name + "]");
                    }
                    builder.searchQuotedAnalyzer(analyzer);
                    iterator.remove();
                } else if (propName.equals("position_increment_gap")
                        || parserContext.indexVersionCreated()
                                .before(Version.V_2_0_0_beta1)
                                && propName.equals("position_offset_gap")) {
                    int newPositionIncrementGap = XContentMapValues
                            .nodeIntegerValue(propNode, -1);
                    if (newPositionIncrementGap < 0) {
                        throw new MapperParsingException(
                                "position_increment_gap less than 0 aren't allowed.");
                    }
                    builder.positionIncrementGap(newPositionIncrementGap);
                    // we need to update to actual analyzers if they are not set in this case...
                    // so we can inject the position increment gap...
                    if (builder.fieldType().indexAnalyzer() == null) {
                        builder.fieldType().setIndexAnalyzer(parserContext
                                .analysisService().defaultIndexAnalyzer());
                    }
                    if (builder.fieldType().searchAnalyzer() == null) {
                        builder.fieldType().setSearchAnalyzer(parserContext
                                .analysisService().defaultSearchAnalyzer());
                    }
                    if (builder.fieldType().searchQuoteAnalyzer() == null) {
                        builder.fieldType().setSearchQuoteAnalyzer(
                                parserContext.analysisService()
                                        .defaultSearchQuoteAnalyzer());
                    }
                    iterator.remove();
                } else if (propName.equals("ignore_above")) {
                    builder.ignoreAbove(
                            XContentMapValues.nodeIntegerValue(propNode, -1));
                    iterator.remove();
                } else if (parseMultiField(builder, name, parserContext,
                        propName, propNode)) {
                    iterator.remove();
                } else if (propName.equals(SEPARATOR_SETTING_KEY)) {
                    builder.fieldSeparator(propNode.toString());
                } else if (propName.equals(LANG_SETTING_KEY)) {
                    builder.supportedLanguages(
                            XContentMapValues.nodeStringArrayValue(propNode));
                }
            }
            return builder;
        }
    }

    public static final class LangStringFieldType extends MappedFieldType {

        public LangStringFieldType() {
        }

        protected LangStringFieldType(LangStringFieldType ref) {
            super(ref);
        }

        public LangStringFieldType clone() {
            return new LangStringFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public String value(Object value) {
            if (value == null) {
                return null;
            }
            return value.toString();
        }

        @Override
        public Query nullValueQuery() {
            if (nullValue() == null) {
                return null;
            }
            return termQuery(nullValue(), null);
        }
    }

    private Boolean includeInAll;

    private int positionIncrementGap;

    private int ignoreAbove;

    private final LangDetectorFactory langDetectorFactory;

    private String fieldSeparator;

    private String[] supportedLanguages;

    private Method parseCopyMethod;

    protected LangStringFieldMapper(String simpleName,
            MappedFieldType fieldType, MappedFieldType defaultFieldType,
            int positionIncrementGap, int ignoreAbove, String fieldSeparator,
            String[] supportedLanguages, Settings indexSettings,
            MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings,
                multiFields, copyTo);
        if (fieldType.tokenized() && fieldType.indexOptions() != NONE
                && fieldType().hasDocValues()) {
            throw new MapperParsingException(
                    "Field [" + fieldType.names().fullName()
                            + "] cannot be analyzed and have doc values");
        }
        this.positionIncrementGap = positionIncrementGap;
        this.ignoreAbove = ignoreAbove;
        this.fieldSeparator = fieldSeparator;
        this.supportedLanguages = supportedLanguages;

        langDetectorFactory = LangDetectorFactory.create(supportedLanguages);

        try {
            Class<?> docParserClazz = FieldMapper.class.getClassLoader()
                    .loadClass("org.elasticsearch.index.mapper.DocumentParser");
            parseCopyMethod = docParserClazz.getDeclaredMethod("parseCopy",
                    new Class[] { String.class, ParseContext.class });
            parseCopyMethod.setAccessible(true);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to access DocumentParser#parseCopy(String, ParseContext).",
                    e);
        }
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
    protected boolean customBoost() {
        return true;
    }

    public int getPositionIncrementGap() {
        return this.positionIncrementGap;
    }

    public int getIgnoreAbove() {
        return ignoreAbove;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields)
            throws IOException {
        ValueAndBoost valueAndBoost = parseCreateFieldForString(context,
                fieldType().nullValueAsString(), fieldType().boost());
        if (valueAndBoost.value() == null) {
            return;
        }
        if (ignoreAbove > 0 && valueAndBoost.value().length() > ignoreAbove) {
            return;
        }
        if (context.includeInAll(includeInAll, this)) {
            context.allEntries().addText(fieldType().names().fullName(),
                    valueAndBoost.value(), valueAndBoost.boost());
        }

        if (fieldType().indexOptions() != IndexOptions.NONE
                || fieldType().stored()) {
            Field field = new Field(fieldType().names().indexName(),
                    valueAndBoost.value(), fieldType());
            field.setBoost(valueAndBoost.boost());
            fields.add(field);
        }
        if (fieldType().hasDocValues()) {
            fields.add(
                    new SortedSetDocValuesField(fieldType().names().indexName(),
                            new BytesRef(valueAndBoost.value())));
        }
        if (fields.isEmpty()) {
            context.ignoredValue(fieldType().names().indexName(),
                    valueAndBoost.value());
        }

        final LangDetector langDetector = langDetectorFactory.getLangDetector();
        langDetector.append(valueAndBoost.value());
        final String lang = langDetector.detect();
        if (!LangDetector.UNKNOWN_LANG.equals(lang)) {
            final String langField = fieldType().names().indexName()
                    + fieldSeparator + lang;
            try {
                parseCopyMethod.invoke(null,
                        new Object[] { langField, context });
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Failed to invoke parseCopy method.", e);
            }
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
                        throw new IllegalArgumentException(
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
    public void merge(Mapper mergeWith, MergeResult mergeResult)
            throws MergeMappingException {
        super.merge(mergeWith, mergeResult);
        if (!this.getClass().equals(mergeWith.getClass())) {
            return;
        }
        if (!mergeResult.simulate()) {
            this.includeInAll = ((LangStringFieldMapper) mergeWith).includeInAll;
            this.ignoreAbove = ((LangStringFieldMapper) mergeWith).ignoreAbove;
            this.fieldSeparator = ((LangStringFieldMapper) mergeWith).fieldSeparator;
            this.supportedLanguages = ((LangStringFieldMapper) mergeWith).supportedLanguages;
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder,
            boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults || fieldType().nullValue() != null) {
            builder.field("null_value", fieldType().nullValue());
        }
        if (includeInAll != null) {
            builder.field("include_in_all", includeInAll);
        } else if (includeDefaults) {
            builder.field("include_in_all", false);
        }

        if (includeDefaults
                || positionIncrementGap != POSITION_INCREMENT_GAP_USE_ANALYZER) {
            builder.field("position_increment_gap", positionIncrementGap);
        }
        NamedAnalyzer searchQuoteAnalyzer = fieldType().searchQuoteAnalyzer();
        if (searchQuoteAnalyzer != null && !searchQuoteAnalyzer.name()
                .equals(fieldType().searchAnalyzer().name())) {
            builder.field("search_quote_analyzer", searchQuoteAnalyzer.name());
        } else if (includeDefaults) {
            if (searchQuoteAnalyzer == null) {
                builder.field("search_quote_analyzer", "default");
            } else {
                builder.field("search_quote_analyzer",
                        searchQuoteAnalyzer.name());
            }
        }
        if (includeDefaults || ignoreAbove != Defaults.IGNORE_ABOVE) {
            builder.field("ignore_above", ignoreAbove);
        }
        if (includeDefaults || !fieldSeparator.equals(FIELD_SEPARATOR)) {
            builder.field(SEPARATOR_SETTING_KEY, fieldSeparator);
        }
        String langs = Strings.arrayToDelimitedString(supportedLanguages, ",");
        if (includeDefaults
                || !langs.equals(Strings.arrayToDelimitedString(SUPPORTED_LANGUAGES, ","))) {
            builder.field(LANG_SETTING_KEY, langs);
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

}
