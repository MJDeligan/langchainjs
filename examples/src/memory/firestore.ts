import { BufferMemory } from "langchain/memory";
import { FirestoreChatMessageHistory } from "@langchain/community/stores/message/firestore";
import { ChatOpenAI } from "@langchain/openai";
import { ConversationChain } from "langchain/chains";

const memory = new BufferMemory({
  chatHistory: new FirestoreChatMessageHistory({
    collectionName: "langchain",
    sessionId: "lc-example",
    userId: "a@example.com",
    config: { projectId: "your-project-id" },
  }),
});

const model = new ChatOpenAI();
const chain = new ConversationChain({ llm: model, memory });

const res1 = await chain.call({ input: "Hi! I'm Jim." });
console.log({ res1 });
/*
{
  res1: {
    text: "Hello Jim! It's nice to meet you. My name is AI. How may I assist you today?"
  }
}
*/

const res2 = await chain.call({ input: "What did I just say my name was?" });
console.log({ res2 });

/*
{
  res1: {
    text: "You said your name was Jim."
  }
}
*/
