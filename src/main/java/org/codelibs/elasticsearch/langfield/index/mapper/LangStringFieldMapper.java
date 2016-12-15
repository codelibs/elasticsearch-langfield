/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.codelibs.elasticsearch.langfield.index.mapper;

import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.index.mapper.TypeParsers.parseTextField;

import java.io.IOException;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.codelibs.elasticsearch.langfield.detect.LangDetector;
import org.codelibs.elasticsearch.langfield.detect.LangDetectorFactory;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.PagedBytesIndexFieldData;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.StringFieldMapper;
import org.elasticsearch.index.mapper.StringFieldType;

/** A {@link FieldMapper} for full-text fields. */
public class LangStringFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "langstring";
    private static final int POSITION_INCREMENT_GAP_USE_ANALYZER = -1;

    private static final List<String> SUPPORTED_PARAMETERS_FOR_AUTO_DOWNGRADE_TO_STRING = unmodifiableList(Arrays.asList(
            "type",
            // common text parameters, for which the upgrade is straightforward
            "index", "store", "doc_values", "omit_norms", "norms", "boost", "fields", "copy_to",
            "fielddata", "eager_global_ordinals", "fielddata_frequency_filter", "include_in_all",
            "analyzer", "search_analyzer", "search_quote_analyzer",
            "index_options", "position_increment_gap", "similarity"));

    private static final String SEPARATOR_SETTING_KEY = "separator";

    private static final String LANG_SETTING_KEY = "lang";

    private static final String LANG_FIELD_SETTING_KEY = "lang_field";

    private static final String LANG_BASE_NAME_SETTING_KEY = "lang_base_name";

    private static final String[] SUPPORTED_LANGUAGES = new String[] { "ar",
            "bg", "bn", "ca", "cs", "da", "de", "el", "en", "es", "et", "fa",
            "fi", "fr", "gu", "he", "hi", "hr", "hu", "id", "it", "ja", "ko",
            "lt", "lv", "mk", "ml", "nl", "no", "pa", "pl", "pt", "ro", "ru",
            "si", "sq", "sv", "ta", "te", "th", "tl", "tr", "uk", "ur", "vi",
            "zh-cn", "zh-tw" };

    private static final String LANG_FIELD = "";

    private static final String FIELD_SEPARATOR = "_";

    private static final String LANG_BASE_NAME = "";

    public static class Defaults {
        public static double FIELDDATA_MIN_FREQUENCY = 0;
        public static double FIELDDATA_MAX_FREQUENCY = Integer.MAX_VALUE;
        public static int FIELDDATA_MIN_SEGMENT_SIZE = 0;

        public static final MappedFieldType FIELD_TYPE = new LangStringFieldType();

        static {
            FIELD_TYPE.freeze();
        }

        /**
         * The default position_increment_gap is set to 100 so that phrase
         * queries of reasonably high slop will not match across field values.
         */
        public static final int POSITION_INCREMENT_GAP = 100;
    }

    public static class Builder extends FieldMapper.Builder<Builder, LangStringFieldMapper> {

        private int positionIncrementGap = POSITION_INCREMENT_GAP_USE_ANALYZER;

        protected String fieldSeparator = FIELD_SEPARATOR;

        protected String[] supportedLanguages = SUPPORTED_LANGUAGES;

        protected String langField = LANG_FIELD;

        protected String langBaseName = LANG_BASE_NAME;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public LangStringFieldType fieldType() {
            return (LangStringFieldType) super.fieldType();
        }

        public Builder positionIncrementGap(int positionIncrementGap) {
            if (positionIncrementGap < 0) {
                throw new MapperParsingException("[positions_increment_gap] must be positive, got " + positionIncrementGap);
            }
            this.positionIncrementGap = positionIncrementGap;
            return this;
        }

        public Builder fielddata(boolean fielddata) {
            fieldType().setFielddata(fielddata);
            return builder;
        }

        @Override
        public Builder docValues(boolean docValues) {
            if (docValues) {
                throw new IllegalArgumentException("[text] fields do not support doc values");
            }
            return super.docValues(docValues);
        }

        public Builder eagerGlobalOrdinals(boolean eagerGlobalOrdinals) {
            fieldType().setEagerGlobalOrdinals(eagerGlobalOrdinals);
            return builder;
        }

        public Builder fieldSeparator(String fieldSeparator) {
            this.fieldSeparator = fieldSeparator;
            return this;
        }

        public Builder supportedLanguages(String[] supportedLanguages) {
            this.supportedLanguages = supportedLanguages;
            return this;
        }

        public Builder langField(String langField) {
            this.langField = langField;
            return this;
        }

        public Builder langBaseName(String langBaseName) {
            this.langBaseName = langBaseName;
            return this;
        }

        public Builder fielddataFrequencyFilter(double minFreq, double maxFreq, int minSegmentSize) {
            fieldType().setFielddataMinFrequency(minFreq);
            fieldType().setFielddataMaxFrequency(maxFreq);
            fieldType().setFielddataMinSegmentSize(minSegmentSize);
            return builder;
        }

        @Override
        public LangStringFieldMapper build(BuilderContext context) {
            if (positionIncrementGap != POSITION_INCREMENT_GAP_USE_ANALYZER) {
                if (fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
                    throw new IllegalArgumentException("Cannot set position_increment_gap on field ["
                        + name + "] without positions enabled");
                }
                fieldType.setIndexAnalyzer(new NamedAnalyzer(fieldType.indexAnalyzer(), positionIncrementGap));
                fieldType.setSearchAnalyzer(new NamedAnalyzer(fieldType.searchAnalyzer(), positionIncrementGap));
                fieldType.setSearchQuoteAnalyzer(new NamedAnalyzer(fieldType.searchQuoteAnalyzer(), positionIncrementGap));
            }
            setupFieldType(context);
            return new LangStringFieldMapper(
                    name, fieldType, defaultFieldType, positionIncrementGap, includeInAll,
                    fieldSeparator, supportedLanguages, langField, langBaseName,
                    context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String fieldName, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            if (parserContext.indexVersionCreated().before(Version.V_5_0_0_alpha1)) {
                // Downgrade "text" to "string" in indexes created in 2.x so you can use modern syntax against old indexes
                Set<String> unsupportedParameters = new HashSet<>(node.keySet());
                unsupportedParameters.removeAll(SUPPORTED_PARAMETERS_FOR_AUTO_DOWNGRADE_TO_STRING);
                if (false == SUPPORTED_PARAMETERS_FOR_AUTO_DOWNGRADE_TO_STRING.containsAll(node.keySet())) {
                    throw new IllegalArgumentException("Automatic downgrade from [text] to [string] failed because parameters "
                            + unsupportedParameters + " are not supported for automatic downgrades.");
                }
                {   // Downgrade "index"
                    Object index = node.get("index");
                    if (index == null || Boolean.TRUE.equals(index)) {
                        index = "analyzed";
                    } else if (Boolean.FALSE.equals(index)) {
                        index = "no";
                    } else {
                        throw new IllegalArgumentException(
                                "Can't parse [index] value [" + index + "] for field [" + fieldName + "], expected [true] or [false]");
                    }
                    node.put("index", index);
                }
                {   // Downgrade "fielddata" (default in string is true, default in text is false)
                    Object fielddata = node.get("fielddata");
                    if (fielddata == null || Boolean.FALSE.equals(fielddata)) {
                        fielddata = false;
                    } else if (Boolean.TRUE.equals(fielddata)) {
                        fielddata = true;
                    } else {
                        throw new IllegalArgumentException("can't parse [fielddata] value for [" + fielddata + "] for field ["
                                + fieldName + "], expected [true] or [false]");
                    }
                    node.put("fielddata", fielddata);
                }

                return new StringFieldMapper.TypeParser().parse(fieldName, node, parserContext);
            }
            LangStringFieldMapper.Builder builder = new LangStringFieldMapper.Builder(fieldName);
            builder.fieldType().setIndexAnalyzer(parserContext.analysisService().defaultIndexAnalyzer());
            builder.fieldType().setSearchAnalyzer(parserContext.analysisService().defaultSearchAnalyzer());
            builder.fieldType().setSearchQuoteAnalyzer(parserContext.analysisService().defaultSearchQuoteAnalyzer());
            parseTextField(builder, fieldName, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals("position_increment_gap")) {
                    int newPositionIncrementGap = XContentMapValues.nodeIntegerValue(propNode, -1);
                    builder.positionIncrementGap(newPositionIncrementGap);
                    iterator.remove();
                } else if (propName.equals("fielddata")) {
                    builder.fielddata(XContentMapValues.nodeBooleanValue(propNode));
                    iterator.remove();
                } else if (propName.equals("eager_global_ordinals")) {
                    builder.eagerGlobalOrdinals(XContentMapValues.nodeBooleanValue(propNode));
                    iterator.remove();
                } else if (propName.equals("fielddata_frequency_filter")) {
                    Map<?,?> frequencyFilter = (Map<?, ?>) propNode;
                    double minFrequency = XContentMapValues.nodeDoubleValue(frequencyFilter.remove("min"), 0);
                    double maxFrequency = XContentMapValues.nodeDoubleValue(frequencyFilter.remove("max"), Integer.MAX_VALUE);
                    int minSegmentSize = XContentMapValues.nodeIntegerValue(frequencyFilter.remove("min_segment_size"), 0);
                    builder.fielddataFrequencyFilter(minFrequency, maxFrequency, minSegmentSize);
                    DocumentMapperParser.checkNoRemainingFields(propName, frequencyFilter, parserContext.indexVersionCreated());
                    iterator.remove();
                } else if (propName.equals(SEPARATOR_SETTING_KEY)) {
                    builder.fieldSeparator(propNode.toString());
                    iterator.remove();
                } else if (propName.equals(LANG_SETTING_KEY)) {
                    builder.supportedLanguages(
                            XContentMapValues.nodeStringArrayValue(propNode));
                    iterator.remove();
                } else if (propName.equals(LANG_FIELD_SETTING_KEY)) {
                    builder.langField(propNode.toString());
                    iterator.remove();
                } else if (propName.equals(LANG_BASE_NAME_SETTING_KEY)) {
                    builder.langBaseName(propNode.toString());
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public static final class LangStringFieldType extends StringFieldType {

        private boolean fielddata;
        private double fielddataMinFrequency;
        private double fielddataMaxFrequency;
        private int fielddataMinSegmentSize;

        public LangStringFieldType() {
            setTokenized(true);
            fielddata = false;
            fielddataMinFrequency = Defaults.FIELDDATA_MIN_FREQUENCY;
            fielddataMaxFrequency = Defaults.FIELDDATA_MAX_FREQUENCY;
            fielddataMinSegmentSize = Defaults.FIELDDATA_MIN_SEGMENT_SIZE;
        }

        protected LangStringFieldType(LangStringFieldType ref) {
            super(ref);
            this.fielddata = ref.fielddata;
            this.fielddataMinFrequency = ref.fielddataMinFrequency;
            this.fielddataMaxFrequency = ref.fielddataMaxFrequency;
            this.fielddataMinSegmentSize = ref.fielddataMinSegmentSize;
        }

        public LangStringFieldType clone() {
            return new LangStringFieldType(this);
        }

        @Override
        public boolean equals(Object o) {
            if (super.equals(o) == false) {
                return false;
            }
            LangStringFieldType that = (LangStringFieldType) o;
            return fielddata == that.fielddata
                    && fielddataMinFrequency == that.fielddataMinFrequency
                    && fielddataMaxFrequency == that.fielddataMaxFrequency
                    && fielddataMinSegmentSize == that.fielddataMinSegmentSize;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), fielddata,
                    fielddataMinFrequency, fielddataMaxFrequency, fielddataMinSegmentSize);
        }

        @Override
        public void checkCompatibility(MappedFieldType other,
                List<String> conflicts, boolean strict) {
            super.checkCompatibility(other, conflicts, strict);
            LangStringFieldType otherType = (LangStringFieldType) other;
            if (strict) {
                if (fielddata() != otherType.fielddata()) {
                    conflicts.add("mapper [" + name() + "] is used by multiple types. Set update_all_types to true to update [fielddata] "
                            + "across all types.");
                }
                if (fielddataMinFrequency() != otherType.fielddataMinFrequency()) {
                    conflicts.add("mapper [" + name() + "] is used by multiple types. Set update_all_types to true to update "
                            + "[fielddata_frequency_filter.min] across all types.");
                }
                if (fielddataMaxFrequency() != otherType.fielddataMaxFrequency()) {
                    conflicts.add("mapper [" + name() + "] is used by multiple types. Set update_all_types to true to update "
                            + "[fielddata_frequency_filter.max] across all types.");
                }
                if (fielddataMinSegmentSize() != otherType.fielddataMinSegmentSize()) {
                    conflicts.add("mapper [" + name() + "] is used by multiple types. Set update_all_types to true to update "
                            + "[fielddata_frequency_filter.min_segment_size] across all types.");
                }
            }
        }

        public boolean fielddata() {
            return fielddata;
        }

        public void setFielddata(boolean fielddata) {
            checkIfFrozen();
            this.fielddata = fielddata;
        }

        public double fielddataMinFrequency() {
            return fielddataMinFrequency;
        }

        public void setFielddataMinFrequency(double fielddataMinFrequency) {
            checkIfFrozen();
            this.fielddataMinFrequency = fielddataMinFrequency;
        }

        public double fielddataMaxFrequency() {
            return fielddataMaxFrequency;
        }

        public void setFielddataMaxFrequency(double fielddataMaxFrequency) {
            checkIfFrozen();
            this.fielddataMaxFrequency = fielddataMaxFrequency;
        }

        public int fielddataMinSegmentSize() {
            return fielddataMinSegmentSize;
        }

        public void setFielddataMinSegmentSize(int fielddataMinSegmentSize) {
            checkIfFrozen();
            this.fielddataMinSegmentSize = fielddataMinSegmentSize;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query nullValueQuery() {
            if (nullValue() == null) {
                return null;
            }
            return termQuery(nullValue(), null);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder() {
            if (fielddata == false) {
                throw new IllegalArgumentException("Fielddata is disabled on text fields by default. Set fielddata=true on [" + name()
                        + "] in order to load fielddata in memory by uninverting the inverted index. Note that this can however "
                        + "use significant memory.");
            }
            return new PagedBytesIndexFieldData.Builder(fielddataMinFrequency, fielddataMaxFrequency, fielddataMinSegmentSize);
        }
    }

    private Boolean includeInAll;
    private int positionIncrementGap;
    private final LangDetectorFactory langDetectorFactory;
    private String fieldSeparator;
    private String[] supportedLanguages;
    private String langField;
    private String langBaseName;
    private Method parseCopyMethod;

    protected LangStringFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                int positionIncrementGap, Boolean includeInAll,
                                String fieldSeparator, String[] supportedLanguages, String langField, String langBaseName,
                                Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        assert fieldType.tokenized();
        assert fieldType.hasDocValues() == false;
        if (fieldType().indexOptions() == IndexOptions.NONE && fieldType().fielddata()) {
            throw new IllegalArgumentException("Cannot enable fielddata on a [text] field that is not indexed: [" + name() + "]");
        }
        this.positionIncrementGap = positionIncrementGap;
        this.includeInAll = includeInAll;
        this.fieldSeparator = fieldSeparator;
        this.supportedLanguages = supportedLanguages;
        this.langField = langField;
        this.langBaseName = langBaseName;

        langDetectorFactory = LangDetectorFactory.create(supportedLanguages);

        parseCopyMethod = AccessController.doPrivileged(new PrivilegedAction<Method>() {
            @Override
            public Method run() {
                try {
                    Class<?> docParserClazz = FieldMapper.class.getClassLoader().loadClass("org.elasticsearch.index.mapper.DocumentParser");
                    Method method = docParserClazz.getDeclaredMethod("parseCopy", new Class[] { String.class, ParseContext.class });
                    method.setAccessible(true);
                    return method;
                } catch (Exception e) {
                    throw new IllegalStateException("Failed to access DocumentParser#parseCopy(String, ParseContext).", e);
                }
            }
        });
    }

    @Override
    protected LangStringFieldMapper clone() {
        return (LangStringFieldMapper) super.clone();
    }

    // pkg-private for testing
    Boolean includeInAll() {
        return includeInAll;
    }

    public int getPositionIncrementGap() {
        return this.positionIncrementGap;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        final String value;
        if (context.externalValueSet()) {
            value = context.externalValue().toString();
        } else {
            value = context.parser().textOrNull();
        }

        if (value == null) {
            return;
        }

        if (context.includeInAll(includeInAll, this)) {
            context.allEntries().addText(fieldType().name(), value, fieldType().boost());
        }

        if (fieldType().indexOptions() != IndexOptions.NONE || fieldType().stored()) {
            Field field = new Field(fieldType().name(), value, fieldType());
            fields.add(field);
        }

        if (value != null && value.trim().length() > 0) {
            final String lang = detectLanguage(context, value);
            if (!LangDetector.UNKNOWN_LANG.equals(lang)) {
                final StringBuilder langFieldBuf = new StringBuilder();
                if (langBaseName.length() == 0) {
                    langFieldBuf.append(fieldType().name());
                } else {
                    langFieldBuf.append(langBaseName);
                }
                langFieldBuf.append(fieldSeparator).append(lang);
                try {
                    parseCopyMethod.invoke(null, new Object[] { langFieldBuf.toString(), context });
                } catch (Exception e) {
                    throw new IllegalStateException(
                            "Failed to invoke parseCopy method.", e);
                }
            }
        }
    }

    private String detectLanguage(final ParseContext context,
            final String text) {
        if (langField != null && langField.length() > 0) {
            final IndexableField[] langFields = context.doc()
                    .getFields(langField);
            if (langFields != null) {
                for (IndexableField langField : langFields) {
                    if (langField instanceof Field) {
                        final BytesRef bytes = langField.binaryValue();
                        if (bytes != null) {
                            final String lang = bytes.utf8ToString();
                            if (lang.length() > 0) {
                                for (final String supportedLang : supportedLanguages) {
                                    if (supportedLang.equals(lang)) {
                                        return lang;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        try {
            final LangDetector langDetector = langDetectorFactory.getLangDetector();
            langDetector.append(text);
            return langDetector.detect();
        } catch (Exception e) {
            // TODO logger
            return LangDetector.UNKNOWN_LANG;
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        super.doMerge(mergeWith, updateAllTypes);
        this.includeInAll = ((LangStringFieldMapper) mergeWith).includeInAll;
        this.fieldSeparator = ((LangStringFieldMapper) mergeWith).fieldSeparator;
        this.supportedLanguages = ((LangStringFieldMapper) mergeWith).supportedLanguages;
        this.langField = ((LangStringFieldMapper) mergeWith).langField;
        this.langBaseName = ((LangStringFieldMapper) mergeWith).langBaseName;
    }

    @Override
    public LangStringFieldType fieldType() {
        return (LangStringFieldType) super.fieldType();
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        doXContentAnalyzers(builder, includeDefaults);

        if (includeInAll != null) {
            builder.field("include_in_all", includeInAll);
        } else if (includeDefaults) {
            builder.field("include_in_all", true);
        }

        if (includeDefaults || positionIncrementGap != POSITION_INCREMENT_GAP_USE_ANALYZER) {
            builder.field("position_increment_gap", positionIncrementGap);
        }

        if (includeDefaults || fieldType().fielddata() != ((LangStringFieldType) defaultFieldType).fielddata()) {
            builder.field("fielddata", fieldType().fielddata());
        }
        if (fieldType().fielddata()) {
            if (includeDefaults
                    || fieldType().fielddataMinFrequency() != Defaults.FIELDDATA_MIN_FREQUENCY
                    || fieldType().fielddataMaxFrequency() != Defaults.FIELDDATA_MAX_FREQUENCY
                    || fieldType().fielddataMinSegmentSize() != Defaults.FIELDDATA_MIN_SEGMENT_SIZE) {
                builder.startObject("fielddata_frequency_filter");
                if (includeDefaults || fieldType().fielddataMinFrequency() != Defaults.FIELDDATA_MIN_FREQUENCY) {
                    builder.field("min", fieldType().fielddataMinFrequency());
                }
                if (includeDefaults || fieldType().fielddataMaxFrequency() != Defaults.FIELDDATA_MAX_FREQUENCY) {
                    builder.field("max", fieldType().fielddataMaxFrequency());
                }
                if (includeDefaults || fieldType().fielddataMinSegmentSize() != Defaults.FIELDDATA_MIN_SEGMENT_SIZE) {
                    builder.field("min_segment_size", fieldType().fielddataMinSegmentSize());
                }
                builder.endObject();
            }
        }
        if (includeDefaults || !fieldSeparator.equals(FIELD_SEPARATOR)) {
            builder.field(SEPARATOR_SETTING_KEY, fieldSeparator);
        }
        String langs = Strings.arrayToDelimitedString(supportedLanguages, ",");
        if (includeDefaults
                || !langs.equals(Strings.arrayToDelimitedString(SUPPORTED_LANGUAGES, ","))) {
            builder.field(LANG_SETTING_KEY, langs);
        }
        if (includeDefaults || !langField.equals(LANG_FIELD)) {
            builder.field(LANG_FIELD_SETTING_KEY, langField);
        }
        if (includeDefaults || !langField.equals(LANG_BASE_NAME)) {
            builder.field(LANG_BASE_NAME_SETTING_KEY, langBaseName);
        }
    }
}
