import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by talwanich on 26/07/2016.
 */
public class ParseNode {

    private Word word = new Word();
    private List<ParseNode> children = new LinkedList<ParseNode>();



    public ParseNode(String word, String partOfSpeech, int sentencePos) {
        this.word.set(word, partOfSpeech, sentencePos);

    }

    public  List<ParseNode> getChildren() {
        return children;
    }

    public String getPath() {
        return word.toString();
    }


    public void addChildren(Hashtable<Integer, LinkedList<ParseNode>> nodes) {

        LinkedList<ParseNode> children = nodes.get(this.word.getSentencePos());
        if(children != null && !children.isEmpty()) {
            this.addChildren(children);
            for (ParseNode child : children)
                child.addChildren(nodes);
        }

    }

    private void addChildren(LinkedList<ParseNode> children) {
        this.children.addAll(children);
    }


    public boolean isNoun() {
        return this.word.isNoun();
    }
}
