package cis5550.jobs;
public class TrieNode {

    public boolean hasWord;
    public int numPrefixes;
    public TrieNode[] children;

    public TrieNode() {
        numPrefixes = 0;
        children = new TrieNode[26];
        hasWord = false;
    }

    public boolean hasWord() {
        return hasWord;
    }
}
