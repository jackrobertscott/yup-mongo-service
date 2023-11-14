import { Collection, Document, Filter, ObjectId, SortDirection } from "mongodb"
import * as yup from "yup"

export function createYupMongoService<
  T extends { id: string },
  X extends Record<string, Function>
>(options: {
  schema: yup.ObjectSchema<T>
  collection: () => Promise<Collection>
  extend: (data: ReturnType<typeof createYupMongoCore<T>>) => X
}) {
  const core = createYupMongoCore<T>(options.schema, options.collection)
  const data = options.extend(core)
  return { ...core, ...data }
}

export function createYupMongoCore<T extends { id: string }>(
  yupSchema: yup.ObjectSchema<T>,
  getCollection: () => Promise<Collection>
) {
  return {
    getCollection,

    getSchema: () => yupSchema,

    async createOne(
      document: Partial<Omit<T, "id">>,
      _id?: ObjectId
    ): Promise<string> {
      const validatedData = await this.getValidatedData(document)
      const collection = await getCollection()
      if (_id) validatedData["_id"] = _id
      if ("id" in validatedData) delete validatedData["id"]
      const result = await collection.insertOne(validatedData)
      return result.insertedId.toString()
    },

    async createMany(
      documents: Array<Partial<Omit<T, "id">>>
    ): Promise<string[]> {
      const validatedData = await Promise.all(
        documents.map((item) => this.getValidatedData(item))
      )
      const collection = await getCollection()
      const operations = validatedData.map((item) => ({
        insertOne: {
          document: item,
        },
      }))
      const result = await collection.bulkWrite(operations)
      return Object.values(result.insertedIds).map((id) => id.toString())
    },

    async patchOneById(
      id: string | ObjectId,
      document: Partial<Omit<T, "id">>
    ): Promise<T | null> {
      const validatedData = await this.getValidatedData(document)
      const collection = await getCollection()
      await collection.updateOne(
        { _id: new ObjectId(id) },
        { $set: validatedData }
      )
      return this.getOneById(id)
    },

    async patchManyById(
      documents: Array<{ id: string | ObjectId; data: Partial<Omit<T, "id">> }>
    ): Promise<void> {
      const collection = await getCollection()
      const operations = await Promise.all(
        documents.map(async ({ id, data }) => {
          const validatedData = await this.getValidatedData(data)
          return {
            updateOne: {
              filter: { _id: new ObjectId(id) },
              update: { $set: validatedData },
            },
          }
        })
      )
      await collection.bulkWrite(operations)
    },

    async deleteOneById(id: string | ObjectId): Promise<boolean> {
      const collection = await getCollection()
      const result = await collection.deleteOne({ _id: new ObjectId(id) })
      return result.deletedCount > 0
    },

    async getOneById(id: string | ObjectId): Promise<T | null> {
      const collection = await getCollection()
      const document = await collection.findOne({ _id: new ObjectId(id) })
      if (!document) return null
      return this.addVirtualId(document)
    },

    async getOne(filter: Filter<Omit<T, "id">>): Promise<T | null> {
      const collection = await getCollection()
      const document = await collection.findOne(filter as Filter<Document>)
      if (!document) return null
      return this.addVirtualId(document)
    },

    async getMany(
      filter: Filter<Omit<T, "id">>,
      options?: {
        limit?: number
        skip?: number
        sort?: { [key: string]: SortDirection }
      }
    ): Promise<T[]> {
      const collection = await getCollection()
      let query = collection.find(filter as any)
      if (options) {
        if (options.limit) query = query.limit(options.limit)
        if (options.skip) query = query.skip(options.skip)
        if (options.sort) query = query.sort(options.sort)
      }
      const documents = await query.toArray()
      return documents.map(this.addVirtualId)
    },
    async getValidatedData(data: any): Promise<any> {
      try {
        return await yupSchema.validate(data, {
          abortEarly: false,
          stripUnknown: true,
          strict: true,
        })
      } catch (error) {
        const message = error instanceof Error ? error.message : "Unknown"
        throw new Error(`Validation error: ${message}`)
      }
    },

    getSchemaWithoutId() {
      return yupSchema.omit(["id"])
    },

    addVirtualId(document: any): T {
      return { ...document, id: document._id.toString() }
    },
  }
}
