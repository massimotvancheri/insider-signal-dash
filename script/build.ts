import { build as esbuild } from "esbuild";
import { build as viteBuild } from "vite";
import { rm, readFile, cp, access, mkdir } from "fs/promises";

// server deps to bundle to reduce openat(2) syscalls
// which helps cold start times
const allowlist = [
  "@google/generative-ai",
  "axios",
  "cors",
  "date-fns",
  "drizzle-orm",
  "drizzle-zod",
  "express",
  "express-rate-limit",
  "express-session",
  "jsonwebtoken",
  "memorystore",
  "multer",
  "nanoid",
  "nodemailer",
  "openai",
  "passport",
  "passport-local",
  "stripe",
  "uuid",
  "ws",
  "xlsx",
  "zod",
  "zod-validation-error",
];

async function buildAll() {
  // Backup existing dist before wiping — restore on failure
  const hasExistingDist = await access("dist/index.cjs").then(() => true).catch(() => false);
  if (hasExistingDist) {
    await rm("dist-backup", { recursive: true, force: true });
    await cp("dist", "dist-backup", { recursive: true });
  }

  try {
    await rm("dist", { recursive: true, force: true });

    console.log("building client...");
    await viteBuild();

    console.log("building server...");
    const pkg = JSON.parse(await readFile("package.json", "utf-8"));
    const allDeps = [
      ...Object.keys(pkg.dependencies || {}),
      ...Object.keys(pkg.devDependencies || {}),
    ];
    const externals = allDeps.filter((dep) => !allowlist.includes(dep));

    await esbuild({
      entryPoints: ["server/index.ts"],
      platform: "node",
      bundle: true,
      format: "cjs",
      outfile: "dist/index.cjs",
      define: {
        "process.env.NODE_ENV": '"production"',
      },
      minify: true,
      external: externals,
      logLevel: "info",
    });

    console.log("building poller...");
    await esbuild({
      entryPoints: ["server/poller.ts"],
      platform: "node",
      bundle: true,
      format: "cjs",
      outfile: "dist/poller.cjs",
      define: {
        "process.env.NODE_ENV": '"production"',
      },
      minify: true,
      external: externals,
      logLevel: "info",
    });
  } catch (err) {
    // Build failed — restore backup if available
    if (hasExistingDist) {
      console.error("Build failed, restoring previous dist/");
      await rm("dist", { recursive: true, force: true });
      await cp("dist-backup", "dist", { recursive: true });
    }
    throw err;
  } finally {
    await rm("dist-backup", { recursive: true, force: true });
  }
}

buildAll().catch((err) => {
  console.error(err);
  process.exit(1);
});
