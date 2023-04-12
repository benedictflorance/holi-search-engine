package cis5550.jobs;

import java.util.HashMap;
import java.util.Map;

public class TrieNode {

    public boolean hasWord;
    public int numPrefixes;
    public Map<Character, TrieNode> children;

    public TrieNode() {
        numPrefixes = 0;
        children = new HashMap<Character, TrieNode>();
        hasWord = false;
    }

    public boolean hasWord() {
        return hasWord;
    }
}
