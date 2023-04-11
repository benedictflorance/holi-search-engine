import React from 'react';

function Logo({className = ''}) {
    return (
        <div className={'w-max font-vollkorn ' + className}>
            <font className="text-goonavy">H</font>
            <font className="text-gooblue">O</font>
            <font className="text-goocyan">L</font>
            <font className="text-goonavy">I</font>
        </div>
    );
}
export default Logo;
