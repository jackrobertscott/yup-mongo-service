import { Collection, MongoClient, ObjectId } from "mongodb"
import { MongoMemoryServer } from "mongodb-memory-server"
import * as yup from "yup"
import { createYupMongoService } from "."

const testSchema = yup.object({
  id: yup.string().required(),
})

describe("MongoDB Integration Tests", () => {
  let mongod: MongoMemoryServer
  let connection: MongoClient
  let db: Collection<any>

  beforeAll(async () => {
    mongod = await MongoMemoryServer.create()
    connection = await MongoClient.connect(mongod.getUri())
    db = connection.db("test").collection("test") // Replace with your collection name
  })

  afterAll(async () => {
    await connection.close()
    await mongod.stop()
  })

  afterEach(async () => {
    await db.deleteMany({})
  })

  it("should create a document in the collection", async () => {
    const collection = async () => db
    const service = createYupMongoService({
      schema: testSchema,
      collection,
      extend: () => ({}),
    })
    const documentData = {
      id: "1",
      field1: "value1",
      field2: "value2",
    }
    const createdId = await service.createOne(documentData)
    const createdDocument = await db.findOne({ _id: new ObjectId(createdId) })
    expect(createdDocument).toBeTruthy()
  })

  it("should update a document by ID", async () => {
    const collection = async () => db
    const service = createYupMongoService({
      schema: testSchema,
      collection,
      extend: () => ({}),
    })
    const initialData = {
      id: "1",
      field1: "value1",
      field2: "value2",
    }
    const createdId = await service.createOne(initialData)
    const updatedData = {
      id: "1", // Include the same 'id' as in initialData
      field1: "updatedValue1",
      field2: "updatedValue2",
    }
    await service.patchOneById(createdId, updatedData)
    const updatedDocument = await db.findOne({ _id: new ObjectId(createdId) })
    expect(updatedDocument).toEqual(expect.objectContaining(updatedData))
  })

  it("should delete a document by ID", async () => {
    const collection = async () => db
    const service = createYupMongoService({
      schema: testSchema,
      collection,
      extend: () => ({}),
    })
    const initialData = {
      id: "1",
      field1: "value1",
      field2: "value2",
    }
    const createdId = await service.createOne(initialData)
    const isDeleted = await service.deleteOneById(createdId)
    expect(isDeleted).toBeTruthy()
    const deletedDocument = await db.findOne({ _id: new ObjectId(createdId) })
    expect(deletedDocument).toBeNull()
  })
})
