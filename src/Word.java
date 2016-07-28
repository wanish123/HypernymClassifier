import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * Created by talwanich on 28/07/2016.
 */
public class Word implements Writable, WritableComparable<Word>{
    private Text word = new Text();
    private Text partOfSpeech = new Text();
    private IntWritable sentencePos = new IntWritable();

    public String getPartOfSpeech() {
        return partOfSpeech.toString();
    }

    public String getWord() {
        return word.toString();
    }

    private enum Nouns {NN, NNS, NNP, NNPS}

    public Word(){}

    @Override
    public int compareTo(Word o) {
        return this.word.compareTo(o.word);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.word.write(dataOutput);
        this.partOfSpeech.write(dataOutput);
        this.sentencePos.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.word.readFields(dataInput);
        this.partOfSpeech.readFields(dataInput);
        this.sentencePos.readFields(dataInput);
    }

    public void set(String word, String partOfSpeech, int sentencePos) {
        this.word.set(word);
        this.partOfSpeech.set(partOfSpeech);
        this.sentencePos.set(sentencePos);
    }

    public void set(String word) {
        this.word.set(word);
    }

    @Override
    public String toString(){
        return this.word.toString() + "/" + this.partOfSpeech.toString();
    }

    public void set(String word, String POS) {
        this.word.set(word);
        this.partOfSpeech.set(POS);
    }

    public int getSentencePos() {
        return sentencePos.get();
    }

    public boolean isNoun() {
        String tag = this.partOfSpeech.toString();
        return tag.equals(Nouns.NN.toString()) ||
                tag.equals(Nouns.NNS.toString()) ||
                tag.equals(Nouns.NNP.toString())||
                tag.equals(Nouns.NNPS.toString());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;

        if (!Word.class.isAssignableFrom(obj.getClass()))
            return false;

        final Word other = (Word) obj;

        if(!(this.word.toString().equals(other.word.toString())))
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return this.word.toString().hashCode();
    }
}
