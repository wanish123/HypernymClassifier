/**
 * Created by talwanich on 26/07/2016.
 */
public class Subsentence {

    private NounPair nounPair;
    private DependencyPath dp;

    public Subsentence(String path) {
        String[] parts = path.split(" ");
        StringBuilder sb = new StringBuilder();
        this.nounPair = new NounPair(parts[0], parts[parts.length-1]);
        for(String p: parts)
            sb.append(p + " ");
        this.dp = new DependencyPath(sb.toString());

    }


    public NounPair getNounPair() {
        return nounPair;
    }

    public DependencyPath getDependencyPath() {
        return dp;
    }
}
