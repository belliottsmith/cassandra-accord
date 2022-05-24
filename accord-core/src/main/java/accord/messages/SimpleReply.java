package accord.messages;

public class SimpleReply implements Reply
{
    private static final SimpleReply OK = new SimpleReply();
    private static final SimpleReply NACK = new SimpleReply();

    public static SimpleReply ok()
    {
        return OK;
    }

    public static SimpleReply nack()
    {
        return NACK;
    }

    private SimpleReply() {}

    @Override
    public MessageType type()
    {
        return MessageType.SIMPLE_RSP;
    }
}
