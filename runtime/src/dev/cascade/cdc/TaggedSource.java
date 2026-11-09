package dev.cascade.cdc;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * Wraps a CDC {@link Source}: every split and enumerator checkpoint is tagged with the slot
 * generation it belongs to. Restored state of another generation is ignored, so the source
 * re-snapshots; its splits then carry {@code resnapshot}, and the reader that gets the stream split
 * (assigned only after every snapshot split finished) emits one marker for {@link Reconcile}.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
final class TaggedSource implements Source<RowData, TaggedSource.Split, TaggedSource.Checkpoint> {
    private final Source inner;
    private final String tag;
    private final int arity;

    TaggedSource(Source<RowData, ?, ?> inner, String tag, int arity) {
        this.inner = inner;
        this.tag = tag;
        this.arity = arity;
    }

    static final class Split implements SourceSplit {
        final String tag;
        final boolean resnapshot;
        final SourceSplit inner;

        Split(String tag, boolean resnapshot, SourceSplit inner) {
            this.tag = tag;
            this.resnapshot = resnapshot;
            this.inner = inner;
        }

        @Override
        public String splitId() {
            return inner.splitId();
        }
    }

    static final class Checkpoint {
        final String tag;
        final boolean resnapshot;
        final Object inner;

        Checkpoint(String tag, boolean resnapshot, Object inner) {
            this.tag = tag;
            this.resnapshot = resnapshot;
            this.inner = inner;
        }
    }

    @Override
    public Boundedness getBoundedness() {
        return inner.getBoundedness();
    }

    @Override
    public SourceReader<RowData, Split> createReader(SourceReaderContext ctx) throws Exception {
        return new Reader(inner.createReader(ctx), tag, arity);
    }

    @Override
    public SplitEnumerator<Split, Checkpoint> createEnumerator(SplitEnumeratorContext<Split> ctx) throws Exception {
        return new Enumerator(inner.createEnumerator(new Context(ctx, tag, false)), tag, false);
    }

    @Override
    public SplitEnumerator<Split, Checkpoint> restoreEnumerator(SplitEnumeratorContext<Split> ctx, Checkpoint cp)
            throws Exception {
        if (tag.equals(cp.tag)) {
            return new Enumerator(inner.restoreEnumerator(new Context(ctx, tag, cp.resnapshot), cp.inner),
                    tag, cp.resnapshot);
        }
        // saved under another slot generation: that position no longer exists, read everything again
        return new Enumerator(inner.createEnumerator(new Context(ctx, tag, true)), tag, true);
    }

    @Override
    public SimpleVersionedSerializer<Split> getSplitSerializer() {
        SimpleVersionedSerializer s = inner.getSplitSerializer();
        return new Wrapped<Split>(s) {
            void write(DataOutputStream out, Split v) throws IOException {
                header(out, v.tag, v.resnapshot);
                body(out, s.serialize(v.inner));
            }

            Split read(DataInputStream in) throws IOException {
                String t = in.readUTF();
                boolean r = in.readBoolean();
                int version = in.readInt();
                return new Split(t, r, (SourceSplit) s.deserialize(version, bytes(in)));
            }
        };
    }

    @Override
    public SimpleVersionedSerializer<Checkpoint> getEnumeratorCheckpointSerializer() {
        SimpleVersionedSerializer s = inner.getEnumeratorCheckpointSerializer();
        return new Wrapped<Checkpoint>(s) {
            void write(DataOutputStream out, Checkpoint v) throws IOException {
                header(out, v.tag, v.resnapshot);
                body(out, s.serialize(v.inner));
            }

            Checkpoint read(DataInputStream in) throws IOException {
                String t = in.readUTF();
                boolean r = in.readBoolean();
                int version = in.readInt();
                return new Checkpoint(t, r, s.deserialize(version, bytes(in)));
            }
        };
    }

    abstract static class Wrapped<T> implements SimpleVersionedSerializer<T> {
        final SimpleVersionedSerializer inner;

        Wrapped(SimpleVersionedSerializer inner) {
            this.inner = inner;
        }

        abstract void write(DataOutputStream out, T v) throws IOException;

        abstract T read(DataInputStream in) throws IOException;

        void header(DataOutputStream out, String tag, boolean resnapshot) throws IOException {
            out.writeUTF(tag);
            out.writeBoolean(resnapshot);
            out.writeInt(inner.getVersion());
        }

        void body(DataOutputStream out, byte[] b) throws IOException {
            out.writeInt(b.length);
            out.write(b);
        }

        byte[] bytes(DataInputStream in) throws IOException {
            byte[] b = new byte[in.readInt()];
            in.readFully(b);
            return b;
        }

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(T v) throws IOException {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bos)) {
                write(out, v);
            }
            return bos.toByteArray();
        }

        @Override
        public T deserialize(int version, byte[] data) throws IOException {
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(data))) {
                return read(in);
            }
        }
    }

    static boolean isStreamSplit(SourceSplit s) {
        try {
            return (Boolean) s.getClass().getMethod("isStreamSplit").invoke(s);
        } catch (ReflectiveOperationException e) {
            return s.getClass().getSimpleName().contains("Stream");
        }
    }

    // --- reader ---------------------------------------------------------------------------------

    static final class Reader implements SourceReader<RowData, Split> {
        private final SourceReader inner;
        private final String tag;
        private final int arity;
        private final Map<String, Boolean> resnapshot = new HashMap<>();
        private boolean started;
        private boolean markerPending;

        Reader(SourceReader inner, String tag, int arity) {
            this.inner = inner;
            this.tag = tag;
            this.arity = arity;
        }

        @Override
        public void start() {
            started = true;
            inner.start();
        }

        @Override
        public void addSplits(List<Split> splits) {
            List<SourceSplit> keep = new ArrayList<>();
            for (Split s : splits) {
                if (!started && !tag.equals(s.tag)) {
                    continue; // restored from another slot generation: the enumerator re-snapshots
                }
                resnapshot.put(s.splitId(), s.resnapshot);
                if (s.resnapshot && isStreamSplit(s.inner)) {
                    markerPending = true; // every snapshot split is done
                }
                keep.add(s.inner);
            }
            if (!keep.isEmpty()) {
                inner.addSplits(keep);
            }
        }

        @Override
        public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
            if (markerPending) {
                markerPending = false;
                GenericRowData marker = new GenericRowData(arity);
                marker.setRowKind(RowKind.UPDATE_BEFORE);
                output.collect(marker);
                return InputStatus.MORE_AVAILABLE;
            }
            return inner.pollNext(output);
        }

        @Override
        public List<Split> snapshotState(long checkpointId) {
            List<Split> out = new ArrayList<>();
            for (Object s : inner.snapshotState(checkpointId)) {
                SourceSplit split = (SourceSplit) s;
                out.add(new Split(tag, resnapshot.getOrDefault(split.splitId(), false), split));
            }
            return out;
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return markerPending ? CompletableFuture.completedFuture(null) : inner.isAvailable();
        }

        @Override
        public void notifyNoMoreSplits() {
            inner.notifyNoMoreSplits();
        }

        @Override
        public void handleSourceEvents(SourceEvent event) {
            inner.handleSourceEvents(event);
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            inner.notifyCheckpointComplete(checkpointId); // CDC confirms the slot position here
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) throws Exception {
            inner.notifyCheckpointAborted(checkpointId);
        }

        @Override
        public void pauseOrResumeSplits(java.util.Collection<String> pause, java.util.Collection<String> resume) {
            inner.pauseOrResumeSplits(pause, resume);
        }

        @Override
        public void close() throws Exception {
            inner.close();
        }
    }

    // --- enumerator -----------------------------------------------------------------------------

    static final class Enumerator implements SplitEnumerator<Split, Checkpoint> {
        private final SplitEnumerator inner;
        private final String tag;
        private final boolean resnapshot;

        Enumerator(SplitEnumerator inner, String tag, boolean resnapshot) {
            this.inner = inner;
            this.tag = tag;
            this.resnapshot = resnapshot;
        }

        @Override
        public void start() {
            inner.start();
        }

        @Override
        public void handleSplitRequest(int subtaskId, String host) {
            inner.handleSplitRequest(subtaskId, host);
        }

        @Override
        public void addSplitsBack(List<Split> splits, int subtaskId) {
            List<SourceSplit> back = new ArrayList<>();
            for (Split s : splits) {
                back.add(s.inner);
            }
            inner.addSplitsBack(back, subtaskId);
        }

        @Override
        public void addReader(int subtaskId) {
            inner.addReader(subtaskId);
        }

        @Override
        public Checkpoint snapshotState(long checkpointId) throws Exception {
            return new Checkpoint(tag, resnapshot, inner.snapshotState(checkpointId));
        }

        @Override
        public void close() throws IOException {
            inner.close();
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            inner.notifyCheckpointComplete(checkpointId);
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) throws Exception {
            inner.notifyCheckpointAborted(checkpointId);
        }

        @Override
        public void handleSourceEvent(int subtaskId, SourceEvent event) {
            inner.handleSourceEvent(subtaskId, event);
        }
    }

    /** The inner enumerator's context: its split assignments are tagged on the way out. */
    static final class Context implements SplitEnumeratorContext {
        private final SplitEnumeratorContext<Split> outer;
        private final String tag;
        private final boolean resnapshot;

        Context(SplitEnumeratorContext<Split> outer, String tag, boolean resnapshot) {
            this.outer = outer;
            this.tag = tag;
            this.resnapshot = resnapshot;
        }

        @Override
        public SplitEnumeratorMetricGroup metricGroup() {
            return outer.metricGroup();
        }

        @Override
        public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
            outer.sendEventToSourceReader(subtaskId, event);
        }

        @Override
        public void sendEventToSourceReader(int subtaskId, int attemptNumber, SourceEvent event) {
            outer.sendEventToSourceReader(subtaskId, attemptNumber, event);
        }

        @Override
        public int currentParallelism() {
            return outer.currentParallelism();
        }

        @Override
        public Map<Integer, ReaderInfo> registeredReaders() {
            return outer.registeredReaders();
        }

        @Override
        public Map<Integer, Map<Integer, ReaderInfo>> registeredReadersOfAttempts() {
            return outer.registeredReadersOfAttempts();
        }

        @Override
        public void assignSplits(SplitsAssignment assignment) {
            Map<Integer, List<Split>> tagged = new HashMap<>();
            for (Object e : assignment.assignment().entrySet()) {
                Map.Entry<Integer, List<SourceSplit>> entry = (Map.Entry<Integer, List<SourceSplit>>) e;
                List<Split> l = new ArrayList<>();
                for (SourceSplit s : entry.getValue()) {
                    l.add(new Split(tag, resnapshot, s));
                }
                tagged.put(entry.getKey(), l);
            }
            outer.assignSplits(new SplitsAssignment<>(tagged));
        }

        @Override
        public void signalNoMoreSplits(int subtask) {
            outer.signalNoMoreSplits(subtask);
        }

        @Override
        public void callAsync(Callable callable, BiConsumer handler) {
            outer.callAsync(callable, handler);
        }

        @Override
        public void callAsync(Callable callable, BiConsumer handler, long initialDelay, long period) {
            outer.callAsync(callable, handler, initialDelay, period);
        }

        @Override
        public void runInCoordinatorThread(Runnable runnable) {
            outer.runInCoordinatorThread(runnable);
        }

        @Override
        public void setIsProcessingBacklog(boolean isProcessingBacklog) {
            outer.setIsProcessingBacklog(isProcessingBacklog);
        }
    }
}
