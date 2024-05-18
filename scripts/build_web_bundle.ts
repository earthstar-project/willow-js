import * as esbuild from "npm:esbuild";
import { denoPlugins } from "@luca/esbuild-deno-loader";

const version = Deno.args[0];

const result = await esbuild.build({
  plugins: [
    ...denoPlugins(),
  ],
  entryPoints: ["./mod.browser-bundle.ts"],
  outfile: `./dist/willow${version ? `-${version}` : ""}.web.js`,
  bundle: true,
  format: "esm",
  platform: "browser",
  sourcemap: "linked",
  minify: true,
  metafile: true,
});

if (result.metafile) {
  await Deno.writeTextFile(
    "./dist/metafile.json",
    JSON.stringify(result.metafile),
  );
}

Deno.exit(0);
