package org.codelibs.elasticsearch.langfield.detect;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.codelibs.elasticsearch.langfield.detect.util.LangProfile;
import org.elasticsearch.ElasticsearchException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for {@link LangDetector} and {@link LangDetectorFactory}.
 * @author Nakatani Shuyo
 *
 */
public class LangDetectorTest {

    private static final String TRAINING_EN = "a a a b b c c d e";

    private static final String TRAINING_FR = "a b b c c c d d d";

    private static final String TRAINING_JA = "\u3042 \u3042 \u3042 \u3044 \u3046 \u3048 \u3048";

    private LangDetectorFactory langDetectorFactory;

    @Before
    public void setUp() throws Exception {
        langDetectorFactory = new LangDetectorFactory();

        
        LangProfile profile_en = new LangProfile("en");
        for (String w : TRAINING_EN.split(" "))
            profile_en.add(w);
        langDetectorFactory.addProfile(profile_en, 0, 3);

        LangProfile profile_fr = new LangProfile("fr");
        for (String w : TRAINING_FR.split(" "))
            profile_fr.add(w);
        langDetectorFactory.addProfile(profile_fr, 1, 3);

        LangProfile profile_ja = new LangProfile("ja");
        for (String w : TRAINING_JA.split(" "))
            profile_ja.add(w);
        langDetectorFactory.addProfile(profile_ja, 2, 3);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public final void testDetector1() throws ElasticsearchException {
        LangDetector detect = langDetectorFactory.getLangDetector();
        detect.append("a");
        assertEquals(detect.detect(), "en");
    }

    @Test
    public final void testDetector2() throws ElasticsearchException {
        LangDetector detect = langDetectorFactory.getLangDetector();
        detect.append("b d");
        assertEquals(detect.detect(), "fr");
    }

    @Test
    public final void testDetector3() throws ElasticsearchException {
        LangDetector detect = langDetectorFactory.getLangDetector();
        detect.append("d e");
        assertEquals(detect.detect(), "en");
    }

    @Test
    public final void testDetector4() throws ElasticsearchException {
        LangDetector detect = langDetectorFactory.getLangDetector();
        detect.append("\u3042\u3042\u3042\u3042a");
        assertEquals(detect.detect(), "ja");
    }

    @Test
    public final void testLangList() throws ElasticsearchException {
        List<String> langList = langDetectorFactory.getLangList();
        assertEquals(langList.size(), 3);
        assertEquals(langList.get(0), "en");
        assertEquals(langList.get(1), "fr");
        assertEquals(langList.get(2), "ja");
    }

    @Test(expected = UnsupportedOperationException.class)
    public final void testLangListException() throws ElasticsearchException {
        List<String> langList = langDetectorFactory.getLangList();
        langList.add("hoge");
        //langList.add(1, "hoge");
    }

}