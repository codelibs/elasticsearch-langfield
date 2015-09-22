package org.codelibs.elasticsearch.langfield.detect;

import java.util.ArrayList;

/**
 * {@link Language} is to store the detected language.
 * {@link LangDetector#getProbabilities()} returns an {@link ArrayList} of {@link Language}s.
 *
 * @see LangDetector#getProbabilities()
 * @author Nakatani Shuyo
 *
 */
public class Language {
    public String lang;

    public double prob;

    public Language(final String lang, final double prob) {
        this.lang = lang;
        this.prob = prob;
    }

    @Override
    public String toString() {
        if (lang == null) {
            return "";
        }
        return lang + ":" + prob;
    }
}
