package org.codelibs.elasticsearch.langfield.detect;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codelibs.elasticsearch.langfield.detect.util.LangProfile;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Language LangDetector Factory Class
 *
 * This class manages an initialization and constructions of {@link LangDetector}.
 *
 * Before using language detection library,
 * load profiles with {@link LangDetectorFactory#loadProfile(String)} method
 * and set initialization parameters.
 *
 * When the language detection,
 * construct LangDetector instance via {@link LangDetectorFactory#create()}.
 * See also {@link LangDetector}'s sample code.
 *
 * <ul>
 * <li>4x faster improvement based on Elmer Garduno's code. Thanks!</li>
 * </ul>
 *
 * @see LangDetector
 * @author Nakatani Shuyo
 * @author shinsuke
 */
public class LangDetectorFactory {
    public Map<String, double[]> wordLangProbMap;

    public List<String> langlist;

    public Long seed = null;

    LangDetectorFactory() {
        wordLangProbMap = new HashMap<>();
        langlist = new ArrayList<>();
    }

    /**
     * Load profiles from specified directory.
     * This method must be called once before language detection.
     *
     * @param profileDirectory profile directory path
     */
    public static LangDetectorFactory create(final File profileDirectory) {
        final LangDetectorFactory factory = new LangDetectorFactory();
        final File[] listFiles = profileDirectory.listFiles();
        if (listFiles == null) {
            throw new LangDetectException(ErrorCode.NeedLoadProfileError,
                    "Not found profile: " + profileDirectory);
        }
        final ObjectMapper mapper = new ObjectMapper();
        final int langsize = listFiles.length;
        int index = 0;
        for (final File file : listFiles) {
            if (file.getName().startsWith(".") || !file.isFile()) {
                continue;
            }
            try (InputStream is = new FileInputStream(file);) {
                final LangProfile profile = mapper.readValue(is,
                        LangProfile.class);
                factory.addProfile(profile, index, langsize);
                index++;
            } catch (final IOException e) {
                throw new LangDetectException(ErrorCode.FileLoadError,
                        "can't open '" + file.getName() + "'", e);
            } catch (final Exception e) {
                throw new LangDetectException(ErrorCode.FormatError,
                        "profile format error in '" + file.getName() + "'", e);
            }
        }
        return factory;
    }

    public static LangDetectorFactory create(final String... langs) {
        final LangDetectorFactory factory = new LangDetectorFactory();
        final ObjectMapper mapper = new ObjectMapper();
        final int langsize = langs.length;
        int index = 0;
        for (final String lang : langs) {
            try (InputStream is = LangDetectorFactory.class
                    .getResourceAsStream("/profiles/" + lang)) {
                if (is == null) {
                    throw new IOException(
                            "'/profiles/" + lang + "' does not exist.");
                }
                final LangProfile profile = mapper.readValue(is,
                        LangProfile.class);
                factory.addProfile(profile, index, langsize);
                index++;
            } catch (final IOException e) {
                throw new LangDetectException(ErrorCode.FileLoadError,
                        "can't open 'profiles/" + lang + "'", e);
            } catch (final Exception e) {
                throw new LangDetectException(ErrorCode.FormatError,
                        "profile format error in 'profiles/" + lang + "'", e);
            }
        }
        return factory;
    }

    /**
     * @param profile
     * @param langsize
     * @param index
     */
    void addProfile(final LangProfile profile, final int index,
            final int langsize) {
        final String lang = profile.name;
        if (langlist.contains(lang)) {
            throw new LangDetectException(ErrorCode.DuplicateLangError,
                    "duplicate the same language profile");
        }
        langlist.add(lang);
        for (final String word : profile.freq.keySet()) {
            if (!wordLangProbMap.containsKey(word)) {
                wordLangProbMap.put(word, new double[langsize]);
            }
            final int length = word.length();
            if (length >= 1 && length <= 3) {
                final double prob = profile.freq.get(word).doubleValue()
                        / profile.nWords[length - 1];
                wordLangProbMap.get(word)[index] = prob;
            }
        }
    }

    /**
     * Construct LangDetector instance
     *
     * @return LangDetector instance
     */
    public LangDetector getLangDetector() {
        if (langlist.size() == 0) {
            throw new LangDetectException(ErrorCode.NeedLoadProfileError,
                    "need to load profiles");
        }
        final LangDetector langDetector = new LangDetector(this);
        return langDetector;
    }

    public void setSeed(final long seed) {
        this.seed = seed;
    }

    public final List<String> getLangList() {
        return Collections.unmodifiableList(this.langlist);
    }
}
