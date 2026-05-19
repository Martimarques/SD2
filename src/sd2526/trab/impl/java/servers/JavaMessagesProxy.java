package sd2526.trab.impl.java.servers;

import java.util.List;
import sd2526.trab.api.Message;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.java.Result;
import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.impl.api.zoho.Zoho;

public class JavaMessagesProxy implements Messages, AdminMessages {

    private static JavaMessagesProxy instance;

    public synchronized static JavaMessagesProxy getInstance() {
        if (instance == null) instance = new JavaMessagesProxy();
        return instance;
    }

    @Override
    public Result<String> postMessage(String pwd, Message msg) {
        try {
            if (msg.getId() == null) msg.setId("zoho-" + System.currentTimeMillis());
            return Result.ok(Zoho.getInstance().sendEmail(msg));
        } catch (Exception e) {
            e.printStackTrace();
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    @Override
    public Result<Void> remotePostMessage(Message msg) {
        try {
            Zoho.getInstance().sendEmail(msg);
            return Result.ok();
        } catch (Exception e) {
            e.printStackTrace();
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    @Override
    public Result<List<String>> getAllInboxMessages(String name, String pwd) {
        try {
            return Result.ok(Zoho.getInstance().getAllMessages(name));
        } catch (Exception e) {
            e.printStackTrace();
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    @Override
    public Result<Message> getInboxMessage(String name, String mid, String pwd) {
        try {
            Message originalMsg = Zoho.getInstance().getMessage(mid);
            if (originalMsg != null) {
                return Result.ok(originalMsg);
            } else {
                return Result.error(Result.ErrorCode.NOT_FOUND);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    @Override
    public Result<Void> removeInboxMessage(String n, String m, String p) {
        System.out.println("Proxy: removeInboxMessage chamado para mid = " + m);
        Zoho.getInstance().deleteMessage(m);
        return Result.ok();
    }

    @Override
    public Result<Void> deleteMessage(String n, String m, String p) {
        System.out.println("Proxy: deleteMessage chamado para mid = " + m);
        Zoho.getInstance().deleteMessage(m);
        return Result.ok();
    }

    @Override
    public Result<Void> remoteDeleteMessage(String mid) {
        System.out.println("Proxy: remoteDeleteMessage chamado para mid = " + mid);
        Zoho.getInstance().deleteMessage(mid);
        return Result.ok();
    }

    @Override
    public Result<List<String>> searchInbox(String n, String p, String q) {
        return Result.ok(List.of());
    }

    @Override
    public Result<Void> remoteDeleteUserInbox(String name) {
        return Result.ok();
    }
}