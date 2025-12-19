import { createPortal } from 'react-dom';
import { cn } from "@/lib/utils";

const LoadingSpinner = ({ fullscreen = true, message = "Loading..." }) => {
    const spinnerContent = (
        <div className="flex flex-col items-center gap-4">
            {/* Blockchain-themed animated spinner */}
            <div className="relative h-16 w-16">
                {/* Outer rotating ring */}
                <div className="absolute inset-0 rounded-full border-4 border-muted animate-spin border-t-[#D40000]"></div>

                {/* Inner pulsing circle */}
                <div className="absolute inset-2 rounded-full bg-[#D40000]/20 animate-pulse"></div>

                {/* Center dot */}
                <div className="absolute inset-0 flex items-center justify-center">
                    <div className="h-3 w-3 rounded-full bg-[#D40000]"></div>
                </div>
            </div>

            {/* Loading text */}
            <p className="text-sm font-medium text-muted-foreground animate-pulse">
                {message}
            </p>
        </div>
    );

    if (fullscreen) {
        // Use portal to render at document.body level, ensuring true fullscreen
        return createPortal(
            <div className="fixed inset-0 z-[9999] flex items-center justify-center bg-background/95 backdrop-blur-sm">
                {spinnerContent}
            </div>,
            document.body
        );
    }

    // Inline spinner for non-fullscreen use
    return (
        <div className="flex items-center justify-center p-8">
            <div className="relative h-12 w-12">
                <div className="absolute inset-0 rounded-full border-4 border-muted animate-spin border-t-[#D40000]"></div>
                <div className="absolute inset-2 rounded-full bg-[#D40000]/20 animate-pulse"></div>
                <div className="absolute inset-0 flex items-center justify-center">
                    <div className="h-2 w-2 rounded-full bg-[#D40000]"></div>
                </div>
            </div>
        </div>
    );
};

export default LoadingSpinner;
