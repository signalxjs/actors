# Build context is the REPO ROOT (workspace deps need the whole tree):
#
#   az acr build --registry <acr> --image sigx-actors-test:<git-sha> \
#     --platform linux/amd64 --file perf/aks/Dockerfile .
#
# The runtime needs Node >= 22.18: the entries import .ts modules via
# built-in type stripping. node:22-bookworm-slim satisfies that today —
# the build fails fast below if the tag ever drifts under 22.18.
FROM node:22-bookworm-slim AS build
RUN node -e "const [maj,min]=process.versions.node.split('.').map(Number); if (maj<22 || (maj===22 && min<18)) { console.error('need Node >= 22.18, got', process.versions.node); process.exit(1); }"
RUN corepack enable && corepack prepare pnpm@10.28.2 --activate
WORKDIR /repo
COPY . .
RUN pnpm install --frozen-lockfile
RUN pnpm build
# A self-contained deployable: prod deps only, workspace packages packed
# from their `files` (= dist). --legacy is the pnpm 10 flag for exactly
# this layout (without inject-workspace-packages).
RUN pnpm --filter sigx-perf-aks deploy --prod --legacy /out

FROM node:22-bookworm-slim
ENV NODE_ENV=production
WORKDIR /app
COPY --from=build /out ./
# Prove the deployed tree resolves the runtime under the prod condition —
# a broken pnpm-deploy layout should fail the image build, not the pod.
RUN node --conditions=production -e "await import('@sigx/actors/host'); await import('@sigx/actors-redis'); await import('@sigx/actors-k8s'); await import('@sigx/actors-ws/node'); await import('@sigx/actors-ws/client'); await import('ws');"
USER node
EXPOSE 7311
# --conditions=production selects dist/*.prod.js in every @sigx package
# (ops() only enforces its bearer secret in the prod build). The load
# generator runs from the same image with the command swapped to
# loadgen.mjs.
CMD ["node", "--conditions=production", "server.mjs"]
