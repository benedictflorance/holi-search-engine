package cis5550.jobs;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Set;
import java.util.TreeSet;

public class Trie {

    public TrieNode root;

    /**
     * The trie structure that stores each alphabet as a node
     */
    public Trie() {
        root = new TrieNode();
    }

    /**
     * Adds a new word to the Trie
     *
     * @param word to be added to the Trie
     */
    public void addWord(String word) {
        addHelper(root, word, 0);
    }

    /**
     * The recursive method to traverse through the trie
     * to add the input to the designated location
     *
     * @param n the current node to store data
     * @param word to be added to the Trie
     * @param pos the pointer that points to the current letter in the word
     */
    private void addHelper(TrieNode n, String word, int pos) {
        if (pos == word.length()) {
            n.hasWord = true;
            n.numPrefixes++;
            return;
        }
        n.numPrefixes++;
        TrieNode child = n.children.get(word.charAt(pos));
        if (child == null) {
        	child = new TrieNode();
            n.children.put(word.charAt(pos), child);
        }
        addHelper(child, word, pos + 1);
    }

    /**
     * @param prefix the prefix to get the sub trie for
     * @return the root of the subTrie corresponding to the last character of the
     *         prefix.
     */
    public TrieNode getSubTrie(String prefix) {
        TrieNode current = root;
        int i = 0;
        while (i < prefix.length()) {
            if (!current.children.containsKey(prefix.charAt(i))) {
                return null;
            }
            current = current.children.get(prefix.charAt(i));
            i++;
        }
        return current;
    }

    /**
     * @param prefix the prefix to search for
     * @return the number of words that start with prefix.
     */
    public int countPrefixes(String prefix) {
        return countPrefixesHelper(prefix, root, 0);
    }

    /**
     * The recursive method to traverse through the trie
     * and count the number of words starting with the prefix
     * @param prefix the prefix to search for
     * @param node to retrieve the number of words matching with the prefix
     * @param pos the pointer that points to the current letter in the word
     * @return the recursive function
     */
    private int countPrefixesHelper(String prefix, TrieNode node, int pos) {
        if (pos == prefix.length()) {
            return node.numPrefixes;
        } else {
            // Get the alphabetical position of the character
            char c = prefix.charAt(pos);
            // If the prefix doesn't have a child, there would be no word
            if (!node.children.containsKey(c)) {
                return 0;
            }
            return countPrefixesHelper(prefix, node.children.get(c), pos + 1);
        }
    }

    /**
     *
     * @param prefix to search for
     * @return a List containing all the words with query starting with
     *         prefix. Return an empty list if there are no IEntry object starting
     *         with prefix.
     */
    public Set<String> getSuggestions(String prefix) {
        Set<String> suggestions = new TreeSet<String>();
        getSuggestionsHelper(suggestions, getSubTrie(prefix), new StringBuilder());
        return suggestions;
    }

    /**
     * The recursive method to traverse through the trie
     * to get the list of words for the suggestion
     * @param set the set to store the words
     * @param node to retrieve the data from
     * @param sb the prefix
     */
    private void getSuggestionsHelper(Set<String> set, TrieNode node, StringBuilder sb) {
        if (node == null) {
            return;
        }
        if (node.hasWord()) {
            set.add(sb.toString());
        }
        for (Character c : node.children.keySet()) {
        	sb.append(c);
            getSuggestionsHelper(set, node.children.get(c), sb);
            sb.setLength(sb.length() - 1);
        	
        }
    }

    /**
     *
     * @param query to search for
     * @return if the trie contains the word
     */
    public boolean containsWord(String query) {
    	 if (Character.isUpperCase(query.charAt(0))) {
             return true;
         }
    	 
    	 //if a query contains both alphabets and digits
    	 if(query.matches(".*[a-zA-Z].*") && query.matches(".*\\d.*")) {
    		 return false;
    	 }
    	 //to account for numbers
    	 if(query.matches(".*\\d.*")&&query.length()<=4) {
    		 return true;
    	 }
    	 return containsWordHelper(query.toLowerCase(), root, 0);
    }

    /**
     * The recursive method to traverse through the trie
     * and check if the word is included in the trie
     * @param query the word to search if it exists or not
     * @param node to retrieve the data
     * @param pos the pointer that points to the current letter in the query
     * @return the recursive function
     */
    private boolean containsWordHelper(String query, TrieNode node, int pos) {
        if (pos == query.length()) {
            return node.hasWord();
        }
        char c = query.charAt(pos);
        if (!node.children.containsKey(c)) {
            return false;
        }
        return containsWordHelper(query, node.children.get(c), pos + 1);
        
    }
    public void buildTrie(String filename) throws Exception {
        FileInputStream fis = new FileInputStream(filename);
        Reader r = new InputStreamReader(fis);
        BufferedReader br = new BufferedReader(r);
        String line = br.readLine();
        while (line != null) {
            if (line.length() == 0) {
                line = br.readLine();
                continue;
            }
            String word = line.trim().toLowerCase();
            addWord(word);
            line = br.readLine();
        }
        br.close();
    }
    
    public static void main(String args[]) {
    	Trie trie = new Trie();
		try {
			trie.buildTrie("src/cis5550/jobs/words_alpha.txt");
			System.out.println(trie.containsWord("hello"));
			System.out.println(trie.containsWord("Hello"));
			System.out.println(trie.containsWord("begf"));
			System.out.println(trie.containsWord("be123gf"));
			System.out.println(trie.containsWord("123456"));
			
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
   
}