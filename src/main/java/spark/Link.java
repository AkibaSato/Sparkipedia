package spark;

/**
 * Created by russ on 4/22/18.
 */
public class Link {
    public String docID;
    public String text;
    public String title;
    public String url;

    public Link (String docID, String url, String text, String title) {
        this.docID = docID;
        this.url = url;
        this.text = text;
        this.title = title;
    }

    public String getDocID() {
        return this.docID;
    }

    public String getText() {
        return this.text;
    }

    public String getTitle() {
        return this.title;
    }

    public String getUrl(){
        return this.url;
    }

    public String toString(){
        return String.format("{%s %s}", docID, text);
    }
}

