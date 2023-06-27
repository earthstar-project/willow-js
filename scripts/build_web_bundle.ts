import * as esbuild from "https://deno.land/x/esbuild@v0.17.19/mod.js";
import { denoPlugins } from "https://deno.land/x/esbuild_deno_loader@0.8.1/mod.ts";

const version = Deno.args[0];

const result = await esbuild.build({
  plugins: [
    ...denoPlugins(),
  ],
  entryPoints: ["./mod.browser.ts"],
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
