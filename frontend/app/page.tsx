import VideoUploader from "@/components/VideoUploader";

export default function Home() {
  return (
    <main className="container mx-auto p-8 flex flex-col items-center">
      <div className="text-center mb-12">
        <h1 className="text-4xl font-bold mb-2">Project Chronos</h1>
        <p className="text-lg text-gray-400">The AI-native application for video content.</p>
      </div>
      <VideoUploader />
    </main>
  );
}
