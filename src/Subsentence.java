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
        for(int i = 1; i < parts.length -1; i++)
            sb.append(parts[i] + " ");
        this.dp = new DependencyPath(sb.toString());

    }


    public NounPair getNounPair() {
        return nounPair;
    }

    public DependencyPath getDependencyPath() {
        return dp;
    }
}
