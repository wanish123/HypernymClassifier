/**
 * Created by talwanich on 26/07/2016.
 */
public class Subsentence {

    private NounPair nounPair;
    private DependencyPath dp;

    public Subsentence(String path) {
        String[] parts = path.split(" ");
        StringBuilder sb = new StringBuilder();
        String[] wordInfo = parts[0].split("/");
        String first = wordInfo[0];
        String POS1 = wordInfo[1];
        wordInfo = parts[parts.length-1].split("/");
        String second = wordInfo[0];
        String POS2 = wordInfo[1];
        this.nounPair = new NounPair(first, POS1, second, POS2);
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
