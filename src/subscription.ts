import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscriptionBase, getOpsByType } from './util/subscription'

const dids = [
		"did:plc:odqmsar3ikz5ubokya4sempk",
		"did:plc:v4h6fr7ke4exrqnj36ozhzln",
		"did:plc:yxtlgh77ufljflscr7jyx72k",
		"did:plc:ra7sf5vqxpkkkxcn2oouvpw2",
		"did:plc:twyd3kl5cok4t62v4cyicx3c",
		"did:plc:4egv5reautg2jfzwff5yslv4",
		"did:plc:uc2dimxyaq2urnkeytu3wf3u",
		"did:plc:cxbrpkkancmwlt25ngi7zmh4",
		"did:plc:rurabrmfzfbq7kplsl3l57hb",
		"did:plc:tjrcgiq5c4xoqzs45hijre6j",
		"did:plc:acaukchnb4wey4apa2dayfoa",
		"did:plc:osnbenf3tpsn4qnvnlbwxjwd",
		"did:plc:lo3hkal6g4u3fxbtyrd6trel",
		"did:plc:m3ccqv7m7dqkpowjtpdf6hyz",
		"did:plc:ry7gem5inu5ply2okjgr7xj2",
		"did:plc:7ayz3poguphxqp3bdwmjcep3",
		"did:plc:3dswxngkt6v364kz576bbg4x",
		"did:plc:hd7fnd3akye2xmhj2afktbpy",
		"did:plc:tkhpduizlwsp3z5ucthd74y7",
		"did:plc:pffxichc3l3hnvyysin3zjbi",
		"did:plc:wxemqpzxkz7gmyq3sg4pvqdy",
		"did:plc:akrye47gtvvme2viudvajp33",
		"did:plc:vxyy6ueehysagudvadpodf5f",
		"did:plc:wlmj476suxgwymj4ryabwioh",
		"did:plc:thaurucesmhg55fu3w42zay6",
		"did:plc:fyfdxadtkzdy5bx63eaegp5z",
		"did:plc:czfice3ke4lhmdxgzloylxha",
		"did:plc:zgzd562ohk7phvh4ewzbddcu",
]

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return
    const ops = await getOpsByType(evt)

    // Log all posts from users in the list
    for (const post of ops.posts.creates) {
      if (post.author in dids) {
        console.log(post.record.text)
        console.log(post.author)
      }
    }

    const postsToDelete = ops.posts.deletes.map((del) => del.uri)
    const postsToCreate = ops.posts.creates
      .filter((create) => {
        // only index posts from users in the list that contain the word "art"
        return create.record.text.toLowerCase().includes('art') &&
                create.author in dids
      })
      .map((create) => {
        // map matched posts to a db row
        return {
          uri: create.uri,
          cid: create.cid,
          replyParent: create.record?.reply?.parent.uri ?? null,
          replyRoot: create.record?.reply?.root.uri ?? null,
          indexedAt: new Date().toISOString(),
        }
      })

    if (postsToDelete.length > 0) {
      await this.db
        .deleteFrom('post')
        .where('uri', 'in', postsToDelete)
        .execute()
    }
    if (postsToCreate.length > 0) {
      await this.db
        .insertInto('post')
        .values(postsToCreate)
        .onConflict((oc) => oc.doNothing())
        .execute()
    }
  }
}
