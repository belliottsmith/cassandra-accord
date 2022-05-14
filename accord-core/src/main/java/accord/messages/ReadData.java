/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package accord.messages;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import accord.api.RoutingKey;
import accord.primitives.*;

import accord.local.*;
import accord.local.Node.Id;
import accord.api.Data;
import accord.topology.Topologies;
import accord.utils.DeterministicIdentitySet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import static accord.local.Status.Committed;
import static accord.local.Status.Executed;
import static accord.messages.MessageType.READ_RSP;
import static accord.messages.ReadData.ReadNack.NotCommitted;
import static accord.messages.ReadData.ReadNack.Redundant;

public class ReadData extends TxnRequest
{
    private static final Logger logger = LoggerFactory.getLogger(ReadData.class);

    // TODO: dedup - can currently have infinite pending reads that will be executed independently
    static class LocalRead implements Listener, TxnOperation
    {
        final TxnId txnId;
        final Node node;
        final Node.Id replyToNode;
        final ReplyContext replyContext;

        Data data;
        boolean isObsolete; // TODO: respond with the Executed result we have stored?
        Set<CommandStore> waitingOn;

        LocalRead(TxnId txnId, Node node, Id replyToNode, ReplyContext replyContext)
        {
            this.txnId = txnId;
            this.node = node;
            this.replyToNode = replyToNode;
            this.replyContext = replyContext;
        }

        @Override
        public Iterable<TxnId> txnIds()
        {
            return Collections.singleton(txnId);
        }

        @Override
        public Iterable<? extends RoutingKey> keys()
        {
            return Collections.emptyList();
        }

        @Override
        public TxnOperation listenerScope(TxnId caller)
        {
            Set<TxnId> ids = new HashSet<>();
            ids.add(txnId);
            ids.add(caller);
            return TxnOperation.scopeFor(ids, keys());
        }

        @Override
        public String toString()
        {
            return "ReadData$LocalRead{" + txnId + '}';
        }

        @Override
        public synchronized void onChange(Command command)
        {
            logger.trace("{}: updating as listener in response to change on {} with status {} ({})",
                         this, command.txnId(), command.status(), command);
            switch (command.status())
            {
                default: throw new IllegalStateException();
                case NotWitnessed:
                case PreAccepted:
                case Accepted:
                case AcceptedInvalidate:
                case Committed:
                    return;

                case Executed:
                case Applied:
                case Invalidated:
                    obsolete();
                case ReadyToExecute:
            }

            command.removeListener(this);
            if (!isObsolete)
                read(command);
        }

        @Override
        public boolean isTransient()
        {
            return true;
        }

        private synchronized void readComplete(CommandStore commandStore, Data result)
        {
            logger.trace("{}: read completed on {}", txnId, commandStore);
            if (result != null)
                data = data == null ? result : data.merge(result);

            waitingOn.remove(commandStore);
            if (waitingOn.isEmpty())
                node.reply(replyToNode, replyContext, new ReadOk(data));
        }

        private void read(Command command)
        {
            logger.trace("{}: executing read", command.txnId());
            command.read((next, throwable) -> {
                if (throwable != null)
                {
                    logger.trace("{}: read failed for {}: {}", txnId, command.commandStore(), throwable);
                    node.reply(replyToNode, replyContext, ReadNack.Error);
                }
                else
                    readComplete(command.commandStore(), next);
            });
        }

        void obsolete()
        {
            if (!isObsolete)
            {
                isObsolete = true;
                node.reply(replyToNode, replyContext, Redundant);
            }
        }

        synchronized ReadNack setup(TxnId txnId, PartialRoute scope, Timestamp executeAt)
        {
            waitingOn = node.collectLocal(scope, executeAt, DeterministicIdentitySet::new);
            // FIXME: fix/check thread safety
            return CommandStore.mapReduce(waitingOn, instance -> {
                        Command command = instance.command(txnId);
                        Status status = command.status();
                        logger.trace("{}: setting up read with status {} on {}", txnId, status, instance);
                        switch (command.status()) {
                            default:
                                throw new IllegalStateException();
                            case NotWitnessed:
                            case PreAccepted:
                            case Accepted:
                            case AcceptedInvalidate:
                            case Committed:
                                instance.progressLog().waiting(txnId, Executed, scope);
                                command.addListener(this);
                                return status == Committed ? null : NotCommitted;

                            case ReadyToExecute:
                                if (!isObsolete)
                                    read(command);
                                return null;

                            case Executed:
                            case Applied:
                            case Invalidated:
                                isObsolete = true;
                                return Redundant;
                        }
                    }, (r1, r2) -> r1 == null || r2 == null
                            ? r1 == null ? r1 : r2
                            : r1.compareTo(r2) >= 0 ? r1 : r2
            );
        }
    }

    public final TxnId txnId;
    public final Timestamp executeAt;

    public ReadData(Node.Id to, Topologies topologies, TxnId txnId, Route route, Timestamp executeAt)
    {
        super(to, topologies, route);
        this.txnId = txnId;
        this.executeAt = executeAt;
    }

    public void process(Node node, Node.Id from, ReplyContext replyContext)
    {
        ReadNack nack = new LocalRead(txnId, node, from, replyContext)
                .setup(txnId, scope(), executeAt);

        if (nack != null)
            node.reply(from, replyContext, nack);
    }

    @Override
    public Iterable<TxnId> txnIds()
    {
        return Collections.singleton(txnId);
    }

    @Override
    public Iterable<? extends RoutingKey> keys()
    {
        return Collections.emptyList();
    }

    @Override
    public MessageType type()
    {
        return MessageType.READ_REQ;
    }

    public interface ReadReply extends Reply
    {
        boolean isOk();
    }

    public enum ReadNack implements ReadReply
    {
        Invalid, NotCommitted, Redundant, Error;

        @Override
        public String toString()
        {
            return "Read" + name();
        }

        @Override
        public MessageType type()
        {
            return READ_RSP;
        }

        @Override
        public boolean isOk()
        {
            return false;
        }

        @Override
        public boolean isFinal()
        {
            return this != NotCommitted;
        }
    }

    public static class ReadOk implements ReadReply
    {
        public final @Nullable Data data;

        public ReadOk(@Nullable Data data)
        {
            this.data = data;
        }

        @Override
        public String toString()
        {
            return "ReadOk{" + data + '}';
        }

        @Override
        public MessageType type()
        {
            return READ_RSP;
        }

        public boolean isOk()
        {
            return true;
        }
    }

    @Override
    public String toString()
    {
        return "ReadData{" +
               "txnId:" + txnId +
               '}';
    }
}
