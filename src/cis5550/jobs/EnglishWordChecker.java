package cis5550.jobs;

import com.swabunga.spell.engine.SpellDictionaryHashMap;
import com.swabunga.spell.event.SpellChecker;

import java.io.File;
import java.io.IOException;

public class EnglishWordChecker {
    private static final String DICTIONARY_FILE = "/Users/namitashukla/Desktop/holi-search-engine/src/cis5550/jobs/words_alpha.txt";
    private SpellChecker spellChecker = new SpellChecker();
    
    public EnglishWordChecker() throws IOException {
        SpellDictionaryHashMap dictionary = new SpellDictionaryHashMap(new File(DICTIONARY_FILE));
        spellChecker = new SpellChecker(dictionary);
    }
    
    public boolean isEnglishWord(String word) {
    	 //to account for proper nouns
    	 if (Character.isUpperCase(word.charAt(0))) {
             return true;
         }
    	 //to account for numbers
    	 if(word.matches(".*\\d.*")) {
    		 return true;
    	 }
        return spellChecker.isCorrect(word);
    }
}