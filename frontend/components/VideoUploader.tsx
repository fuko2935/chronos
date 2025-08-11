'use client';

import { useState, DragEvent, ChangeEvent, useEffect } from 'react';

const CHUNK_SIZE = 5 * 1024 * 1024; // 5MB

export default function VideoUploader() {
  const [isDragOver, setIsDragOver] = useState(false);
  const [file, setFile] = useState<File | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [uploading, setUploading] = useState(false);
  const [progress, setProgress] = useState(0);
  const [taskId, setTaskId] = useState<string | null>(null);
  const [processingStatus, setProcessingStatus] = useState<string | null>(null);

  const handleDragOver = (e: DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setIsDragOver(true);
  };

  const handleDragLeave = (e: DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setIsDragOver(false);
  };

  const handleDrop = (e: DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setIsDragOver(false);
    const files = e.dataTransfer.files;
    if (files && files.length > 0) {
      handleFile(files[0]);
    }
  };

  const handleFileChange = (e: ChangeEvent<HTMLInputElement>) => {
    const files = e.target.files;
    if (files && files.length > 0) {
      handleFile(files[0]);
    }
  };

  const handleFile = (selectedFile: File) => {
    // Basic validation (e.g., file type)
    if (!selectedFile.type.startsWith('video/')) {
      setError('Invalid file type. Please select a video file.');
      setFile(null);
      return;
    }
    // 2GB file size limit as per PRD
    if (selectedFile.size > 2 * 1024 * 1024 * 1024) {
        setError('File is too large. The maximum file size is 2GB.');
        setFile(null);
        return;
    }
    setError(null);
    setFile(selectedFile);
    setProgress(0);
    setTaskId(null);
    setProcessingStatus(null);
  };

  useEffect(() => {
    if (!taskId) return;

    const interval = setInterval(async () => {
      const response = await fetch(`/api/upload/status/${taskId}`);
      const data = await response.json();

      if (data.state === 'PROGRESS') {
        setProcessingStatus(data.info.status || 'Processing...');
      } else if (data.state === 'SUCCESS') {
        setProcessingStatus('Processing complete!');
        clearInterval(interval);
      } else if (data.state === 'FAILURE') {
        setProcessingStatus(`Processing failed: ${data.info.status || 'Unknown error'}`);
        clearInterval(interval);
      }
    }, 3000);

    return () => clearInterval(interval);
  }, [taskId]);

  const uploadFile = async () => {
    if (!file) {
      setError("No file selected.");
      return;
    }

    setUploading(true);
    setError(null);
    setProgress(0);

    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
    const objectKey = `${Date.now()}_${file.name}`;

    try {
      // 1. Initialize multipart upload
      const initResponse = await fetch('/api/upload/initialize', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ objectKey }),
      });
      const { uploadId } = await initResponse.json();

      if (!uploadId) {
        throw new Error("Failed to initialize upload.");
      }

      // 2. Get pre-signed URLs and upload chunks
      const uploadPromises = [];
      const uploadedParts = [];

      for (let i = 0; i < totalChunks; i++) {
        const start = i * CHUNK_SIZE;
        const end = Math.min(start + CHUNK_SIZE, file.size);
        const chunk = file.slice(start, end);
        const partNumber = i + 1;

        const partResponse = await fetch('/api/upload/part', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ objectKey, uploadId, partNumber }),
        });
        const { url } = await partResponse.json();

        const uploadPromise = fetch(url, {
          method: 'PUT',
          body: chunk,
        }).then(async (res) => {
          if (!res.ok) throw new Error(`Upload of part ${partNumber} failed.`);
          const etag = res.headers.get('ETag');
          if (!etag) throw new Error(`ETag not found for part ${partNumber}.`);
          uploadedParts.push({ PartNumber: partNumber, ETag: etag.replace(/"/g, '') });
          setProgress(prev => prev + (1 / totalChunks) * 100);
        });
        uploadPromises.push(uploadPromise);
      }

      await Promise.all(uploadPromises);

      // 3. Complete multipart upload
      const completeResponse = await fetch('/api/upload/complete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ objectKey, uploadId, parts: uploadedParts }),
      });

      const result = await completeResponse.json();
      if (completeResponse.ok) {
        setTaskId(result.taskId);
        setProcessingStatus("Upload complete. Waiting for processing to start...");
      } else {
        throw new Error(result.error || "Failed to complete upload.");
      }

    } catch (err: any) {
      setError(err.message || "An unknown error occurred during upload.");
    } finally {
      setUploading(false);
    }
  };

  const openFileDialog = () => {
    document.getElementById('file-input')?.click();
  };

  return (
    <div className="flex flex-col items-center justify-center w-full max-w-lg mx-auto">
      <div
        onClick={openFileDialog}
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
        onDrop={handleDrop}
        className={`w-full h-64 border-2 border-dashed rounded-lg cursor-pointer flex items-center justify-center text-center p-4 transition-colors
          ${isDragOver ? 'border-green-500 bg-gray-700' : 'border-gray-500 hover:border-green-400'}
        `}
      >
        <input
          id="file-input"
          type="file"
          className="hidden"
          accept="video/*"
          onChange={handleFileChange}
        />
        <div className="flex flex-col items-center">
          <svg className="w-10 h-10 mb-3 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M7 16a4 4 0 01-4-4V6a4 4 0 014-4h10a4 4 0 014 4v6a4 4 0 01-4 4H7z"></path><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M12 11v6m0 0l-3-3m3 3l3-3"></path></svg>
          <p className="mb-2 text-sm text-gray-400">
            <span className="font-semibold">Click to upload</span> or drag and drop
          </p>
          <p className="text-xs text-gray-500">MP4, MOV, MKV, etc. (Max 2GB)</p>
        </div>
      </div>
      {error && <p className="mt-2 text-sm text-red-500">{error}</p>}
      {file && !error && (
        <div className="mt-4 w-full text-left">
          <p className="text-sm font-semibold">Selected file:</p>
          <p className="text-sm text-gray-300">{file.name} ({(file.size / 1024 / 1024).toFixed(2)} MB)</p>

          <button
            onClick={uploadFile}
            disabled={uploading}
            className="w-full mt-4 px-4 py-2 bg-green-600 text-white rounded-md hover:bg-green-700 disabled:bg-gray-500"
          >
            {uploading ? `Uploading... ${Math.round(progress)}%` : 'Upload Video'}
          </button>

          {uploading && (
            <div className="w-full bg-gray-600 rounded-full h-2.5 mt-2">
              <div className="bg-green-500 h-2.5 rounded-full" style={{ width: `${progress}%` }}></div>
            </div>
          )}
          {processingStatus && (
            <div className="mt-4 w-full text-left p-4 bg-gray-800 rounded-md">
              <p className="text-sm font-semibold">Processing Status:</p>
              <p className="text-sm text-gray-300 whitespace-pre-wrap">{processingStatus}</p>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
