package org.codelibs.elasticsearch.langfield.detect.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * {@link LangProfile} is a Language Profile Class.
 * Users don't use this class directly.
 *
 * @author Nakatani Shuyo
 * @author shinsuke
 */
public class LangProfile {
    private static final int MINIMUM_FREQ = 2;

    private static final int LESS_FREQ_RATIO = 100000;

    @JsonProperty("name")
    public String name = null;

    @JsonProperty("freq")
    public Map<String, Integer> freq = new HashMap<>();

    @JsonProperty("n_words")
    public int[] nWords = new int[NGram.N_GRAM];

    public LangProfile() {
    }

    /**
     * Normal Constructor
     * @param name language name
     */
    public LangProfile(final String name) {
        this.name = name;
    }

    /**
     * Add n-gram to profile
     * @param gram
     */
    public void add(final String gram) {
        if (name == null || gram == null) {
            return; // Illegal
        }
        final int len = gram.length();
        if (len < 1 || len > NGram.N_GRAM) {
            return; // Illegal
        }
        ++nWords[len - 1];
        if (freq.containsKey(gram)) {
            freq.put(gram, freq.get(gram) + 1);
        } else {
            freq.put(gram, 1);
        }
    }

    /**
     * Eliminate below less frequency n-grams and noise Latin alphabets
     */
    public void omitLessFreq() {
        if (name == null) {
            return; // Illegal
        }
        int threshold = nWords[0] / LESS_FREQ_RATIO;
        if (threshold < MINIMUM_FREQ) {
            threshold = MINIMUM_FREQ;
        }

        final Set<String> keys = freq.keySet();
        int roman = 0;
        for (final Iterator<String> i = keys.iterator(); i.hasNext();) {
            final String key = i.next();
            final int count = freq.get(key);
            if (count <= threshold) {
                nWords[key.length() - 1] -= count;
                i.remove();
            } else if (key.matches("^[A-Za-z]$")) {
                roman += count;
            }
        }

        // roman check
        if (roman < nWords[0] / 3) {
            final Set<String> keys2 = freq.keySet();
            for (final Iterator<String> i = keys2.iterator(); i.hasNext();) {
                final String key = i.next();
                if (key.matches(".*[A-Za-z].*")) {
                    nWords[key.length() - 1] -= freq.get(key);
                    i.remove();
                }
            }

        }
    }

    /**
     * Update the language profile with (fragmented) text.
     * Extract n-grams from text and add their frequency into the profile.
     * @param text (fragmented) text to extract n-grams
     */
    public void update(String text) {
        if (text == null) {
            return;
        }
        text = NGram.normalize_vi(text);
        final NGram gram = new NGram();
        for (int i = 0; i < text.length(); ++i) {
            gram.addChar(text.charAt(i));
            for (int n = 1; n <= NGram.N_GRAM; ++n) {
                add(gram.get(n));
            }
        }
    }
}
