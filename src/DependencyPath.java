import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by talwanich on 26/07/2016.
 */
public class DependencyPath implements Writable, WritableComparable<DependencyPath> {
    private Text path = new Text();

    public DependencyPath(){

    }

    public DependencyPath(String path){
        this.path.set(path);
    }

    public boolean isEmpty() {

        return path.toString().isEmpty() || path.toString().equals(" ");
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;

        if (!DependencyPath.class.isAssignableFrom(obj.getClass()))
            return false;

        final DependencyPath other = (DependencyPath) obj;
        if(!path.toString().equals(other.path.toString()))
            return false;

        return true;
    }

    @Override
    public int hashCode() {

        return path.toString().hashCode();
    }

    //Comparing path by length such that the shortest the path t
    // he "higher" it is in terms of order
    @Override
    public int compareTo(DependencyPath o) {
        Integer thisSize = new Integer(this.path.toString().length());
        Integer otherSize = new Integer(o.path.toString().length());
        return otherSize.compareTo(thisSize);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        path.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        path.readFields(dataInput);
    }

    @Override
    public String toString() {
        return path.toString();
    }

    public int length() {
        return this.path.toString().split(" ").length;

    }
}
