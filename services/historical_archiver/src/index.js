const { startArchiver } = require("./archiverService");

async function bootstrap() {
  try {
    await startArchiver();
  } catch (error) {
    console.error("[ARCHIVER] Error:", error);
    process.exit(1);
  }
}

bootstrap();