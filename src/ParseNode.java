import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by talwanich on 26/07/2016.
 */
public class ParseNode {
    private String word;
    private String tag;
    private int sentencePos;

    private List<ParseNode> children = new LinkedList<ParseNode>();

    public boolean isLeaf() {
        return children.isEmpty();
    }

    public void trim() {
        children.clear();

    }

    private enum Nouns {NN, NNS, NNP, NNPS}

    public ParseNode(String word, String tag, int sentencePos) {
        this.word = word;
        this.tag = tag;
        this.sentencePos = sentencePos;
    }

    public  List<ParseNode> getChildren() {
        return children;
    }

    public String getPath() {
        return word + "/" + tag;
    }



    public void addChildren(Hashtable<Integer, LinkedList<ParseNode>> nodes) {

        LinkedList<ParseNode> children = nodes.get(this.sentencePos);
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
        return tag.equals(Nouns.NN.toString()) ||
                tag.equals(Nouns.NNS.toString()) ||
                tag.equals(Nouns.NNP.toString())||
                tag.equals(Nouns.NNPS.toString());
    }
}
