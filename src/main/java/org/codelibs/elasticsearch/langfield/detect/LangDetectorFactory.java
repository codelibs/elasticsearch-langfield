package org.codelibs.elasticsearch.langfield.detect;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codelibs.elasticsearch.langfield.detect.util.LangProfile;
import org.elasticsearch.ElasticsearchException;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Language LangDetector Factory Class
 *
 * This class manages an initialization and constructions of {@link LangDetector}.
 *
 * Before using language detection library,
 * load profiles with {@link LangDetectorFactory#create(String[])} method
 * and set initialization parameters.
 *
 * When the language detection,
 * construct LangDetector instance via {@link LangDetectorFactory#getLangDetector()}.
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
     * @return LangDetectorFactory
     */
    public static LangDetectorFactory create(final File profileDirectory) {
        final LangDetectorFactory factory = new LangDetectorFactory();
        final File[] listFiles = profileDirectory.listFiles();
        if (listFiles == null) {
            throw new ElasticsearchException("Not found profile: " + profileDirectory);
        }
        final ObjectMapper mapper = new ObjectMapper();
        final int langsize = listFiles.length;
        int index = 0;
        for (final File file : listFiles) {
            if (file.getName().startsWith(".") || !file.isFile()) {
                continue;
            }
            final LangProfile profile = AccessController.doPrivileged((PrivilegedAction<LangProfile>) () -> {
                try (InputStream is = new FileInputStream(file);) {
                    return mapper.readValue(is, LangProfile.class);
                } catch (final IOException e1) {
                    throw new ElasticsearchException("can't open '" + file.getName() + "'", e1);
                } catch (final Exception e2) {
                    throw new ElasticsearchException("profile format error in '" + file.getName() + "'", e2);
                }
            });
            factory.addProfile(profile, index, langsize);
            index++;
        }
        return factory;
    }

    public static LangDetectorFactory create(final String... langs) {
        final LangDetectorFactory factory = new LangDetectorFactory();
        final ObjectMapper mapper = new ObjectMapper();
        final int langsize = langs.length;
        int index = 0;
        for (final String lang : langs) {
            final LangProfile profile = AccessController.doPrivileged((PrivilegedAction<LangProfile>) () -> {
                try (InputStream is = LangDetectorFactory.class.getResourceAsStream("/profiles/" + lang)) {
                    if (is == null) {
                        throw new IOException("'/profiles/" + lang + "' does not exist.");
                    }
                    return mapper.readValue(is, LangProfile.class);
                } catch (final IOException e1) {
                    throw new ElasticsearchException("can't open 'profiles/" + lang + "'", e1);
                } catch (final Exception e2) {
                    throw new ElasticsearchException("profile format error in 'profiles/" + lang + "'", e2);
                }
            });
            factory.addProfile(profile, index, langsize);
            index++;
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
            throw new ElasticsearchException("duplicate the same language profile");
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
            throw new ElasticsearchException("need to load profiles");
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
