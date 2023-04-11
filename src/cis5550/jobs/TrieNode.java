package cis5550.jobs;
public class TrieNode {

    public boolean hasWord;
    private int numWords;
    private int numPrefixes;
    public TrieNode[] children;

    public TrieNode() {
        numWords = 0;
        numPrefixes = 0;
        children = new TrieNode[26];
        hasWord = false;
    }

    public boolean hasWord() {
        return hasWord;
    }

    public int getNumWords() {
        return numWords;
    }

    public void setNumWords(int n) {
        numWords = n;
    }

    public int getNumPrefixes() {
        return numPrefixes;
    }

    public void setNumPrefixes(int p) {
        numPrefixes = p;
    }

    public TrieNode[] getChildren() {
        return children;
    }

    public void setChildren(TrieNode[] children) {
        this.children = children;
    }
}
