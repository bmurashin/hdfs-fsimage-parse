package fsimage.parse;

import de.m3y.hadoop.hdfs.hfsa.core.FsImageData;
import de.m3y.hadoop.hdfs.hfsa.core.FsImageLoader;
import de.m3y.hadoop.hdfs.hfsa.core.FsVisitor;
import de.m3y.hadoop.hdfs.hfsa.util.FsUtil;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.jetbrains.annotations.NotNull;


import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;


// https://github.com/marcelmay/hfsa

public class OrcMultiThreadWriter {

    static int[] index = new int[1000];
    static int next = 0;

    public static void main(String @NotNull [] args) throws IOException {

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        System.out.println(formatter.format(System.currentTimeMillis()) + ": Start");

        Configuration conf = new Configuration();
        TypeDescription schema = TypeDescription.fromString("struct<Path:string,Replication:int,ModificationTime:bigint,AccessTime:bigint,PreferredBlockSize:int,BlocksCount:int,FileSize:bigint,NSQUOTA:bigint,DSQUOTA:bigint,Permission:string,UserName:string,GroupName:string>");

        Writer[] orcWriters = new Writer[1000];
        VectorizedRowBatch[] orcBatches = new VectorizedRowBatch[1000];

        System.out.println(formatter.format(System.currentTimeMillis()) + ": Start loading FSImage");

        RandomAccessFile file = new RandomAccessFile(args[0], "r");

        // Load file into memory
        FsImageData fsimageData = new FsImageLoader.Builder()
                .parallel().build()
                .load(file);

        System.out.println(formatter.format(System.currentTimeMillis()) + ": Finished loading FSImage");


        // Traverse file hierarchy
        new FsVisitor.Builder()
                .parallel()
                .visit(fsimageData, new FsVisitor(){
                    Object lock = new Object();

                        @Override
                        public void onFile(FsImageProto.INodeSection.INode inode, String path) {
                            String fileName = ("/".equals(path) ? path : path + '/') + inode.getName().toStringUtf8();
                            FsImageProto.INodeSection.INodeFile f = inode.getFile();
                            PermissionStatus p = fsimageData.getPermissionStatus(f.getPermission());
                            long size = FsUtil.getFileSize(f);

                            int threadId = (int) Thread.currentThread().getId();
                            int threadIndex = ArrayUtils.indexOf(index, threadId);
                            Writer orcWriter = null;
                            VectorizedRowBatch batch = null;
                            if (threadIndex == -1) {
                                try {
                                    synchronized (lock) {
                                        orcWriter = OrcFile.createWriter(new Path(args[1] + threadId + ".orc"),
                                                OrcFile.writerOptions(conf)
                                                        .compress(CompressionKind.SNAPPY)
                                                        .setSchema(schema));
                                        batch = schema.createRowBatch();

                                        orcWriters[next] = orcWriter;
                                        orcBatches[next] = batch;
                                        index[next] = threadId;
                                        next++;
                                    }
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                            else
                            {
                                orcWriter = orcWriters[threadIndex];
                                batch = orcBatches[threadIndex];
                            }

                            int row = batch.size++;

                            ((BytesColumnVector) batch.cols[0]).setVal(row, fileName.getBytes(StandardCharsets.UTF_8));
                            ((LongColumnVector) batch.cols[1]).vector[row] = f.getReplication();
                            ((LongColumnVector) batch.cols[2]).vector[row] = f.getModificationTime();
                            ((LongColumnVector) batch.cols[3]).vector[row] = f.getAccessTime();
                            ((LongColumnVector) batch.cols[4]).vector[row] = f.getPreferredBlockSize();
                            ((LongColumnVector) batch.cols[5]).vector[row] = f.getBlocksCount();
                            ((LongColumnVector) batch.cols[6]).vector[row] = size;
                            ((LongColumnVector) batch.cols[7]).vector[row] = 0;
                            ((LongColumnVector) batch.cols[8]).vector[row] = 0;
                            ((BytesColumnVector) batch.cols[9]).setVal(row, p.getPermission().toString().getBytes(StandardCharsets.UTF_8));
                            ((BytesColumnVector) batch.cols[10]).setVal(row, p.getUserName().getBytes(StandardCharsets.UTF_8));
                            ((BytesColumnVector) batch.cols[11]).setVal(row, p.getGroupName().getBytes(StandardCharsets.UTF_8));

                            if (batch.size == batch.getMaxSize()) {
                                try {
                                    orcWriter.addRowBatch(batch);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                                batch.reset();
                            }

                        }

                        @Override
                        public void onDirectory(FsImageProto.INodeSection.INode inode, String path) {
                            String dirName = ("/".equals(path) ? path : path + '/') + inode.getName().toStringUtf8();
                            FsImageProto.INodeSection.INodeDirectory d = inode.getDirectory();
                            PermissionStatus p = fsimageData.getPermissionStatus(d.getPermission());

                            int threadId = (int) Thread.currentThread().getId();
                            int threadIndex = ArrayUtils.indexOf(index, threadId);
                            Writer orcWriter = null;
                            VectorizedRowBatch batch = null;
                            if (threadIndex == -1) {
                                try {
                                    synchronized (lock) {
                                        orcWriter = OrcFile.createWriter(new Path(args[1] + threadId + ".orc"),
                                                OrcFile.writerOptions(conf)
                                                        .compress(CompressionKind.SNAPPY)
                                                        .setSchema(schema));
                                        batch = schema.createRowBatch();

                                        orcWriters[next] = orcWriter;
                                        orcBatches[next] = batch;
                                        index[next] = threadId;
                                        next++;
                                    }
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                            else
                            {
                                orcWriter = orcWriters[threadIndex];
                                batch = orcBatches[threadIndex];
                            }

                            int row = batch.size++;

                            ((BytesColumnVector) batch.cols[0]).setVal(row, dirName.getBytes(StandardCharsets.UTF_8));
                            ((LongColumnVector) batch.cols[1]).vector[row] = 0;
                            ((LongColumnVector) batch.cols[2]).vector[row] = d.getModificationTime();
                            ((LongColumnVector) batch.cols[3]).vector[row] = 0;
                            ((LongColumnVector) batch.cols[4]).vector[row] = 0;
                            ((LongColumnVector) batch.cols[5]).vector[row] = 0;
                            ((LongColumnVector) batch.cols[6]).vector[row] = 0;
                            ((LongColumnVector) batch.cols[7]).vector[row] = d.getNsQuota();
                            ((LongColumnVector) batch.cols[8]).vector[row] = d.getDsQuota();
                            ((BytesColumnVector) batch.cols[9]).setVal(row, p.getPermission().toString().getBytes(StandardCharsets.UTF_8));
                            ((BytesColumnVector) batch.cols[10]).setVal(row, p.getUserName().getBytes(StandardCharsets.UTF_8));
                            ((BytesColumnVector) batch.cols[11]).setVal(row, p.getGroupName().getBytes(StandardCharsets.UTF_8));

                            if (batch.size == batch.getMaxSize()) {
                                try {
                                    orcWriter.addRowBatch(batch);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                                batch.reset();
                            }

                        }

                        @Override
                        public void onSymLink(FsImageProto.INodeSection.INode inode, String path) {
                            // Do something
                        }
                    }
                );

        for (int i=0; i<index.length; i++) {
            if (i < next) {
                Writer orcWriter = orcWriters[i];
                VectorizedRowBatch batch = orcBatches[i];
                if (batch.size != 0) {
                    orcWriter.addRowBatch(batch);
                    batch.reset();
                }
                orcWriter.close();
            }
        }

        System.out.println(formatter.format(System.currentTimeMillis()) + ": Done");

    }
}
