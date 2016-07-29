
import java.util.Hashtable;
import java.util.LinkedList;

/**
 * Created by talwanich on 26/07/2016.
 */
public class ParseTree {

    private ParseNode root = null;

    public ParseTree(String sentence) {
        Hashtable<Integer, LinkedList<ParseNode>> nodes = new Hashtable<Integer, LinkedList<ParseNode>>();
        parseNodes(sentence, nodes);
        buildTree(nodes);
        

    }

    private void buildTree(Hashtable<Integer, LinkedList<ParseNode>> nodes) throws NullPointerException, ArrayIndexOutOfBoundsException {

        root = nodes.get(0).get(0);
        root.addChildren(nodes);

    }

    private void parseNodes(String sentence, Hashtable<Integer, LinkedList<ParseNode>> nodes) throws NullPointerException, ArrayIndexOutOfBoundsException{
        String[] parts = sentence.split(" ");
        for(int i = 0; i < parts.length; i++){
            String wordInfo = parts[i];
            String[] wordInfoParts = wordInfo.split("/");
            int index;
            try {
                index = Integer.parseInt(wordInfoParts[2]);
            }catch (NumberFormatException e){
                System.out.println(e.getMessage());
                continue;
            }
            ParseNode node = new ParseNode(wordInfoParts[0], wordInfoParts[1],i + 1);
            if(!nodes.containsKey(index))
                nodes.put(index, new LinkedList<ParseNode>());
            nodes.get(index).add(node);

        }
    }

    private boolean isLegal(String sentence) {
        String[] parts = sentence.split(" ");
        for(String part: parts){
            String[] wordInfo = part.split("/");
            if(wordInfo.length < 4)
                return false;
        }
        return true;
    }

    public ParseNode getRoot() {
        return root;
    }
}
