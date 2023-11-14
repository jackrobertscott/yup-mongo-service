import { Collection, Document, Filter, ObjectId, SortDirection } from "mongodb"
import * as yup from "yup"

export type YupMongoService<
  T extends { id: string },
  X extends Record<string, Function>
> = ReturnType<typeof createYupMongoService<T, X>>

/**
 * Creates a service with MongoDB integration and Yup validation.
 * @param options - Configuration options for the service.
 * @returns An object containing MongoDB service methods.
 */
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

export type YupMongoCore<T extends { id: string }> = ReturnType<
  typeof createYupMongoCore<T>
>

/**
 * Core factory function for creating MongoDB services with Yup validation.
 * @param yupSchema - The Yup schema for data validation.
 * @param getCollection - Function to retrieve the MongoDB collection.
 * @returns An object with core MongoDB operations and validation.
 */
export function createYupMongoCore<T extends { id: string }>(
  yupSchema: yup.ObjectSchema<T>,
  getCollection: () => Promise<Collection>
) {
  return {
    /**
     * Gets the MongoDB collection.
     */
    getCollection,

    /**
     * Gets the Yup schema.
     */
    getSchema: () => yupSchema,

    /**
     * Creates a single document in the collection.
     * @param document - The document to create, excluding the ID.
     * @param _id - Optional MongoDB ObjectId for the document.
     * @returns The ID of the created document.
     */
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

    /**
     * Creates multiple documents in the collection.
     * @param documents - An array of documents to create, excluding their IDs.
     * @returns An array of IDs of the created documents.
     */
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

    /**
     * Updates a single document by ID.
     * @param id - The ID of the document to update.
     * @param document - The partial document data for update.
     * @returns The updated document, or null if not found.
     */
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

    /**
     * Updates multiple documents by their IDs.
     * @param documents - An array containing document IDs and their partial data for update.
     */
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

    /**
     * Deletes a single document by ID.
     * @param id - The ID of the document to delete.
     * @returns True if a document was deleted, false otherwise.
     */
    async deleteOneById(id: string | ObjectId): Promise<boolean> {
      const collection = await getCollection()
      const result = await collection.deleteOne({ _id: new ObjectId(id) })
      return result.deletedCount > 0
    },

    /**
     * Retrieves a single document by ID.
     * @param id - The ID of the document to retrieve.
     * @returns The document if found, or null otherwise.
     */
    async getOneById(id: string | ObjectId): Promise<T | null> {
      const collection = await getCollection()
      const document = await collection.findOne({ _id: new ObjectId(id) })
      if (!document) return null
      return this.addVirtualId(document)
    },

    /**
     * Retrieves a single document based on a filter.
     * @param filter - The filter criteria for the document.
     * @returns The document if found, or null otherwise.
     */
    async getOne(filter: Filter<Omit<T, "id">>): Promise<T | null> {
      const collection = await getCollection()
      const document = await collection.findOne(filter as Filter<Document>)
      if (!document) return null
      return this.addVirtualId(document)
    },

    /**
     * Retrieves multiple documents based on a filter and options.
     * @param filter - The filter criteria for the documents.
     * @param options - Query options like limit, skip, and sort.
     * @returns An array of documents that match the criteria.
     */
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

    /**
     * Validates data against the Yup schema.
     * @param data - The data to validate.
     * @returns The validated data.
     */
    async getValidatedData(data: any): Promise<any> {
      return this.getSchemaWithoutId().validate(data, {
        abortEarly: false,
        stripUnknown: true,
        strict: true,
      })
    },

    /**
     * Retrieves the schema without the ID field.
     * @returns The Yup schema excluding the ID field.
     */
    getSchemaWithoutId() {
      return yupSchema.omit(["id"])
    },

    /**
     * Adds a virtual 'id' field to a document.
     * @param document - The document to add the 'id' field to.
     * @returns The document with the added 'id' field.
     */
    addVirtualId(document: any): T {
      return { ...document, id: document._id.toString() }
    },
  }
}
