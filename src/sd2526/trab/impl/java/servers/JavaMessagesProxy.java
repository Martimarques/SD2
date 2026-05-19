package sd2526.trab.impl.java.servers;

import java.util.List;
import sd2526.trab.api.Message;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.java.Result;
import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.impl.api.zoho.Zoho;

// AQUI ESTÁ A CORREÇÃO: Tem de implementar Messages E AdminMessages
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
            System.out.println("Proxy: postMessage chamado para " + msg.getId());
            return Result.ok(Zoho.getInstance().sendEmail(msg));
        } catch (Exception e) {
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    @Override
    public Result<Void> remotePostMessage(Message msg) {
        try {
            System.out.println("Proxy: remotePostMessage recebido! ID: " + msg.getId());
            Zoho.getInstance().sendEmail(msg);
            return Result.ok();
        } catch (Exception e) {
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    @Override
    public Result<List<String>> getAllInboxMessages(String name, String pwd) {
        try {
            // O nosso Zoho.java já foi programado para devolver a lista de Subjects (que são os IDs)
            List<String> mids = Zoho.getInstance().getAllMessages();
            System.out.println("Proxy: Listagem devolveu: " + mids);
            return Result.ok(mids);
        } catch (Exception e) {
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    // --- Métodos obrigatórios das interfaces (retornam NOT_IMPLEMENTED ou OK para não dar erro de compilação) ---

    @Override
    public Result<Message> getInboxMessage(String n, String m, String p) {
        return Result.error(Result.ErrorCode.NOT_IMPLEMENTED);
    }

    @Override
    public Result<Void> removeInboxMessage(String n, String m, String p) {
        return Result.ok();
    }

    @Override
    public Result<Void> deleteMessage(String n, String m, String p) {
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

    @Override
    public Result<Void> remoteDeleteMessage(String mid) {
        return Result.ok();
    }
}