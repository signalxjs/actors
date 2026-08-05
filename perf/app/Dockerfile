# Build context is the REPO ROOT (workspace deps need the whole tree):
#
#   az acr build --registry <acr> --image sigx-chat:<git-sha> \
#     --platform linux/amd64 --file examples/chat/Dockerfile .
#
# Node >= 22.18 for built-in type stripping (the runtime entries import .ts
# app modules through the emitted server bundle's externals).
FROM node:22-bookworm-slim AS build
RUN node -e "const [maj,min]=process.versions.node.split('.').map(Number); if (maj<22 || (maj===22 && min<18)) { console.error('need Node >= 22.18, got', process.versions.node); process.exit(1); }"
RUN corepack enable && corepack prepare pnpm@10.28.2 --activate
WORKDIR /repo
COPY . .
RUN pnpm install --frozen-lockfile
# Recursive + topological: builds every @sigx package AND chat's vite dist
# (dist/client + dist/server) inside the image.
RUN pnpm build
# Self-contained deployable. chat's package.json `files` includes dist/ —
# pnpm deploy packs by publish rules and dist/ is gitignored, so without
# that field the image would ship no build output.
RUN pnpm --filter chat-example deploy --prod --legacy /out

FROM node:22-bookworm-slim
ENV NODE_ENV=production
WORKDIR /app
COPY --from=build /out ./
# Fail the BUILD, not the pod: the runtime deps must resolve under the
# production condition, and the built app must actually be in the image.
RUN node --conditions=production -e "await import('@sigx/actors/host'); await import('@sigx/actors-redis'); await import('@sigx/server/node'); await import('@sigx/server-renderer/node');"
RUN node -e "const fs=require('node:fs'); fs.accessSync('dist/server/entry-server.js'); fs.accessSync('dist/client/index.html')"
USER node
EXPOSE 8080 7311
CMD ["node", "--conditions=production", "server.mjs"]
