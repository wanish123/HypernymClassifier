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
        int i;
        for(i = 1; i < parts.length - 2; i++)
            sb.append(parts[i] + " ");

        if(i == parts.length - 2)
            sb.append(parts[i]);

        this.dp = new DependencyPath(sb.toString().substring(0));

    }


    public NounPair getNounPair() {
        return nounPair;
    }

    public DependencyPath getDependencyPath() {
        return dp;
    }
}
