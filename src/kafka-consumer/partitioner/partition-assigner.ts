import { PartitionAssigner, AssignerProtocol } from "kafkajs";

export const CustomPartitionAssigner = (topic: string, consumer_id_prefix: string) : PartitionAssigner =>
  ({ cluster }) => ({
    name: "CustomPartitionAssigner",
    version: 1,
    protocol({ topics }) {
      return {
        name: this.name,
        metadata: AssignerProtocol.MemberMetadata.encode({
          version: this.version,
          topics,
          userData: Buffer.from([]),
        }),
      };
    },
    assign: async ({ members }) => {
      await cluster.connect();
      await cluster.refreshMetadata();
      const partitionMetadata = cluster.findTopicPartitionMetadata(topic);
      const availablePartitions = partitionMetadata.map((pm) => pm.partitionId);
      const sortedMembers = members.map(({ memberId }) => memberId).sort();
      let assignment : any | undefined  = [];

      for (let i = 0; i < availablePartitions.length; i++) {
        // Partition mapping logic
        let consumerId = `${consumer_id_prefix}${i}`;  //Assign partition index to clientId index name
        let assignee : any = sortedMembers.find(i => i.startsWith(consumerId)); //Select matching member
        // Partition mapping logic

        assignment[assignee] = {};
        assignment[assignee][topic] = [];
        assignment[assignee][topic].push(i);
    }
    console.log('assignment', assignment);
    return Object.keys(assignment).map(memberId => ({
        memberId,
        memberAssignment: AssignerProtocol.MemberAssignment.encode({
            version: 1,
            assignment: assignment[memberId],
            userData: Buffer.from([]),
        }),
    }))
    },
});

