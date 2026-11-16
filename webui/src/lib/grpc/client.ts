import { createClient } from "@connectrpc/connect";
import { createGrpcWebTransport } from "@connectrpc/connect-web";
import { AdminService } from "./gen/admin_pb";
import { createStorageFetch } from "./storage-fetch";

const transport = createGrpcWebTransport({
  baseUrl: window.location.origin,
  fetch: createStorageFetch((input, init) => fetch(input, init)),
});

export const adminClient = createClient(AdminService, transport);
